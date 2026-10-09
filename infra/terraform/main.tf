data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 5
  special = false
  upper   = false
}

locals {
  name    = "${var.prefix}-${var.environment}"
  compact = "${var.prefix}${var.environment}${random_string.suffix.result}"

  # One container per layer: separate permissions (bronze, which holds identifiers, is the most restricted)
  layers = ["landing", "bronze", "silver", "gold", "quarantine", "audit"]

  # Order of the steps in the ADF pipeline; each one calls `housing <step>` on Databricks
  steps = [
    { name = "bronze", parameters = ["bronze"] },
    { name = "silver_references", parameters = ["silver", "--table", "references"] },
    { name = "silver_families", parameters = ["silver", "--table", "families"] },
    { name = "silver_persons", parameters = ["silver", "--table", "persons"] },
    { name = "silver_beneficiaries", parameters = ["silver", "--table", "beneficiaries"] },
    { name = "gold", parameters = ["gold"] },
    { name = "ml", parameters = ["ml"] },
  ]
  tags = merge(var.tags, {
    project       = "housing-lakehouse"
    environment   = var.environment
    personal_data = "yes"
    managed_by    = "terraform"
  })
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

# ------------------------------------------------------------------ Data Lake (ADLS Gen2)
resource "azurerm_storage_account" "lake" {
  name                              = "st${local.compact}"
  resource_group_name               = azurerm_resource_group.rg.name
  location                          = azurerm_resource_group.rg.location
  account_tier                      = "Standard"
  account_replication_type          = var.environment == "prod" ? "ZRS" : "LRS"
  account_kind                      = "StorageV2"
  is_hns_enabled                    = true
  min_tls_version                   = "TLS1_2"
  https_traffic_only_enabled        = true
  allow_nested_items_to_be_public   = false
  shared_access_key_enabled         = false
  infrastructure_encryption_enabled = true
  tags                              = local.tags
}

resource "azurerm_role_assignment" "deployer_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each           = toset(local.layers)
  name               = each.key
  storage_account_id = azurerm_storage_account.lake.id
  depends_on         = [azurerm_role_assignment.deployer_lake]
}

# Raw files with identifiers do not stay in landing forever
resource "azurerm_storage_management_policy" "retention" {
  storage_account_id = azurerm_storage_account.lake.id

  rule {
    name    = "landing-retention"
    enabled = true
    filters {
      prefix_match = ["landing/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        delete_after_days_since_modification_greater_than = var.landing_retention_days
      }
    }
  }
}

# ------------------------------------------------------------------ Key Vault (pseudonymization key)
resource "azurerm_key_vault" "kv" {
  name                       = "kv${local.compact}"
  location                   = azurerm_resource_group.rg.location
  resource_group_name        = azurerm_resource_group.rg.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  purge_protection_enabled   = true
  soft_delete_retention_days = 90
  tags                       = local.tags
}

resource "azurerm_role_assignment" "deployer_kv" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "random_password" "pseudonymization_key" {
  length  = 64
  special = false
}

resource "azurerm_key_vault_secret" "pseudonymization_key" {
  name         = "pseudonymization-key"
  value        = random_password.pseudonymization_key.result
  key_vault_id = azurerm_key_vault.kv.id
  content_type = "hmac-sha256"
  depends_on   = [azurerm_role_assignment.deployer_kv]

  lifecycle {
    # Changing the key changes every pseudonym: rotation is a planned procedure, not an apply.
    ignore_changes = [value]
  }
}

# ------------------------------------------------------------------ Databricks + Unity Catalog
resource "azurerm_databricks_workspace" "dbw" {
  name                        = "dbw-${local.name}"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location
  sku                         = "premium" # Unity Catalog: column-level permissions and tags
  managed_resource_group_name = "rg-${local.name}-dbw-managed"
  tags                        = local.tags
}

resource "azurerm_databricks_access_connector" "uc" {
  name                = "dbac-${local.name}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "uc_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

# ------------------------------------------------------------------ Data Factory (orchestration)
resource "azurerm_data_factory" "adf" {
  name                = "adf-${local.name}-${random_string.suffix.result}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "adf_databricks" {
  scope                = azurerm_databricks_workspace.dbw.id
  role_definition_name = "Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

resource "azurerm_data_factory_linked_service_azure_databricks" "dbw" {
  name                       = "ls_databricks"
  data_factory_id            = azurerm_data_factory.adf.id
  description                = "Databricks through the ADF managed identity (no token)"
  adb_domain                 = "https://${azurerm_databricks_workspace.dbw.workspace_url}"
  msi_work_space_resource_id = azurerm_databricks_workspace.dbw.id

  new_cluster_config {
    node_type             = var.databricks_node_type
    cluster_version       = var.databricks_runtime
    min_number_of_workers = 1
    max_number_of_workers = 4

    spark_environment_variables = {
      PSEUDONYMIZATION_KEY = "{{secrets/housing-lakehouse/pseudonymization-key}}"
      HOUSING_INPUT        = "abfss://landing@${azurerm_storage_account.lake.name}.dfs.core.windows.net/cadunico"
      HOUSING_LAKEHOUSE    = "abfss://{layer}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/housing"
    }
  }
}

resource "azurerm_data_factory_pipeline" "lakehouse" {
  name            = "pl_housing_lakehouse"
  data_factory_id = azurerm_data_factory.adf.id
  description     = "bronze → silver → gold → ml, one Databricks activity per step"

  activities_json = jsonencode([
    for i, step in local.steps : {
      name = step.name
      type = "DatabricksSparkPython"
      dependsOn = [
        for previous in slice(local.steps, max(i - 1, 0), i) : {
          activity             = previous.name
          dependencyConditions = ["Succeeded"]
        }
      ]
      policy = {
        timeout                = "0.04:00:00"
        retry                  = 2
        retryIntervalInSeconds = 600
      }
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.dbw.name
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        pythonFile = var.databricks_entry_script
        parameters = concat(step.parameters, ["--run-id", "@pipeline().RunId"])
        libraries  = [{ whl = var.databricks_wheel }]
      }
    }
  ])
}

resource "azurerm_data_factory_trigger_schedule" "monthly" {
  name            = "tr_monthly"
  data_factory_id = azurerm_data_factory.adf.id
  pipeline_name   = azurerm_data_factory_pipeline.lakehouse.name
  frequency       = "Month"
  interval        = 1
  activated       = var.environment == "prod"
  time_zone       = "E. South America Standard Time"

  schedule {
    days_of_month = [12]
    hours         = [6]
    minutes       = [0]
  }
}

# ------------------------------------------------------------------ Observability
resource "azurerm_log_analytics_workspace" "logs" {
  name                = "log-${local.name}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "PerGB2018"
  retention_in_days   = 90
  tags                = local.tags
}

resource "azurerm_monitor_diagnostic_setting" "adf" {
  name                           = "diag-adf"
  target_resource_id             = azurerm_data_factory.adf.id
  log_analytics_workspace_id     = azurerm_log_analytics_workspace.logs.id
  log_analytics_destination_type = "Dedicated"

  enabled_log {
    category = "PipelineRuns"
  }
  enabled_log {
    category = "ActivityRuns"
  }
  enabled_log {
    category = "TriggerRuns"
  }
}

# Who read the pseudonymization key, and when (audit trail required by the LGPD)
resource "azurerm_monitor_diagnostic_setting" "kv" {
  name                       = "diag-kv"
  target_resource_id         = azurerm_key_vault.kv.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.logs.id

  enabled_log {
    category = "AuditEvent"
  }
}

resource "azurerm_monitor_action_group" "data" {
  name                = "ag-${local.name}"
  resource_group_name = azurerm_resource_group.rg.name
  short_name          = "data"
  tags                = local.tags

  email_receiver {
    name          = "data-team"
    email_address = var.alert_email
  }
}

resource "azurerm_monitor_metric_alert" "pipeline_failure" {
  name                = "pipeline-failure-${local.name}"
  resource_group_name = azurerm_resource_group.rg.name
  scopes              = [azurerm_data_factory.adf.id]
  description         = "A lakehouse pipeline run failed"
  severity            = 1
  frequency           = "PT5M"
  window_size         = "PT15M"
  tags                = local.tags

  criteria {
    metric_namespace = "Microsoft.DataFactory/factories"
    metric_name      = "PipelineFailedRuns"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  action {
    action_group_id = azurerm_monitor_action_group.data.id
  }
}
