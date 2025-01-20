resource "azurerm_linux_virtual_machine" "nifi_vm" {
  name                = "nifi-vm"
  resource_group_name = azurerm_resource_group.nifi_rg.name
  location            = azurerm_resource_group.nifi_rg.location
  size                = "Standard_DS2_v2"
  admin_username      = "azureuser"
  network_interface_ids = [
    azurerm_network_interface.nifi_nic.id
  ]

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "20_04-lts"
    version   = "latest"
  }

  custom_data = filebase64("cloud-init.yaml")
}
