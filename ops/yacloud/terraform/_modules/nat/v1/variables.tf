variable "folder_id" {
  type = string
}

variable "network_id" {
  type = string
}

variable "locations" {
  type = list(object({
    subnet_id = string
    zone      = string
  }))
}

variable "nat_instance_vm_config" {
  type = object({
    platform_id   = string
    cores         = number
    core_fraction = number
    memory        = number
  })
}

variable "nat_subnet_cidrs" {
  type = list(list(string))
}
