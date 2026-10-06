variable "subscription_id" {
  description = "Azure subscription"
  type        = string
}

variable "prefix" {
  description = "Resource name prefix (lowercase letters and digits, up to 12 characters)"
  type        = string
  default     = "housing"

  validation {
    condition     = can(regex("^[a-z0-9]{3,12}$", var.prefix))
    error_message = "Use 3 to 12 lowercase letters or digits."
  }
}

variable "environment" {
  description = "dev or prod"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "The environment must be dev or prod."
  }
}

variable "location" {
  type    = string
  default = "brazilsouth"
}

variable "alert_email" {
  description = "Who receives pipeline failure alerts"
  type        = string
}

variable "landing_retention_days" {
  description = "Days before raw landing files are deleted (LGPD: deletion once processing ends)"
  type        = number
  default     = 90
}

variable "databricks_runtime" {
  description = "Databricks Runtime version of the job clusters"
  type        = string
  default     = "15.4.x-scala2.12"
}

variable "databricks_node_type" {
  type    = string
  default = "Standard_D4ds_v5"
}

variable "databricks_wheel" {
  description = "Path of the project wheel (published by CI to a Unity Catalog volume)"
  type        = string
  default     = "/Volumes/housing/artifacts/wheels/housing_lakehouse-0.1.0-py3-none-any.whl"
}

variable "databricks_entry_script" {
  description = "Entry script called by ADF"
  type        = string
  default     = "/Volumes/housing/artifacts/scripts/run_step.py"
}

variable "tags" {
  type    = map(string)
  default = {}
}
