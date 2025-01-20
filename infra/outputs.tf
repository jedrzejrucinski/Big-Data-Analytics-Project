output "resource_group_name" {
  value = module.resource_group.name
}

output "vm_public_ip" {
  value = azurerm_public_ip.main.ip_address
}

output "storage_account_name" {
  value = azurerm_storage_account.main.name
}

output "containers" {
  value = [
    azurerm_storage_container.weather_historical.name,
    azurerm_storage_container.satellite_historical.name,
    azurerm_storage_container.models.name
  ]
}
