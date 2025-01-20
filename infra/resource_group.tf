resource "azurerm_resource_group" "main" {
  name     = "nifi-resource-group"
  location = "North Europe"
}

output "name" {
  value = azurerm_resource_group.main.name
}
