resource "azurerm_storage_account" "main" {
  name                     = "bigdataprojectstorage1"
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "weather_historical" {
  name                  = "weather-historical"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "satellite_historical" {
  name                  = "satellite-historical"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "models" {
  name                  = "models"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}
