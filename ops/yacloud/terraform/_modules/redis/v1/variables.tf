variable "name" {
  type = string
}

variable "network_id" {
  type = string
}

variable "redis_version" {
  type = string
}

variable "preset" {
  type = string
}

variable "disk_size" {
  type = number
}

variable "disable_autopurge" {
  type = bool
}

variable "locations" {
  type = list(map(string))
}

variable "password" {
  type = string
}

variable "security_group_ids" {
  type    = list(string)
  default = []
}
