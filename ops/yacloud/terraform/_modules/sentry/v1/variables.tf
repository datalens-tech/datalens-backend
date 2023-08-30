variable "network_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "dns_zone_id" {
  type = string
}

variable "alb_security_group_id" {
  type = string
}

variable "postgresql_config" {
  type = object({
    preset             = string
    security_group_ids = list(string)
    locations = list(object({
      subnet_id = string
      zone      = string
    }))
  })
}

variable "sentry_version" {
  type = string
}