variable "location" {
  default = "North Europe"
}

variable "resource_group_name" {
  type = string
}

variable "network_interface_ids" {
  type = list(string)
}
