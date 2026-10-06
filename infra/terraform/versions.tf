terraform {
  required_version = ">= 1.6"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Remote state in a dedicated storage account with restricted access: the state holds the
  # pseudonymization key generated here. Configure with `terraform init -backend-config=backend.hcl`.
  backend "azurerm" {}
}

provider "azurerm" {
  subscription_id     = var.subscription_id
  storage_use_azuread = true # the lake rejects shared access keys, Entra ID only

  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
}
