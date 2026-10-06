output "storage_account" {
  value = azurerm_storage_account.lake.name
}

output "lake_dfs_endpoint" {
  value = azurerm_storage_account.lake.primary_dfs_endpoint
}

output "key_vault_uri" {
  value = azurerm_key_vault.kv.vault_uri
}

output "databricks_url" {
  value = "https://${azurerm_databricks_workspace.dbw.workspace_url}"
}

output "databricks_access_connector_id" {
  description = "Use when creating the Unity Catalog storage credential"
  value       = azurerm_databricks_access_connector.uc.id
}

output "data_factory" {
  value = azurerm_data_factory.adf.name
}
