provider "azurerm" {
  features {}
  subscription_id = ""
}

module "resource_group" {
  source = "./resource_group"
}

module "network" {
  source = "./network"
  resource_group_name = module.resource_group.name
}

module "vm" {
  source = "./vm"
  resource_group_name = module.resource_group.name
  network_interface_ids = module.network.network_interface_ids
}

module "storage" {
  source = "./storage"
  resource_group_name = module.resource_group.name
}
