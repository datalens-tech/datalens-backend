variable "yc_profile" {
  type        = string
  description = "yc-preprod / yc-prod"
}

variable "env_name" {
  type = string
}

variable "k8s_cluster_region" {
  type = string
}

variable "k8s_version" {
  type = string
}

variable "setup_logbroker" {
  type    = bool
  default = true
}

variable "logbroker_name" {
  type = string
}

variable "enable_kafka" {
  type = bool
}

variable "kafka_settings" {
  type = object({
    disk_size                    = number
    public_ip                    = bool
    resource_preset_id           = string
    version                      = string
    zones                        = list(string)
    brokers_count                = number
    zookeeper_resource_preset_id = string
  })
  nullable = true
}

locals {
  v4_healthchecks_cidrs = [
    "198.18.235.0/24",
    "198.18.248.0/24",
  ]
  v6_healthchecks_cidrs = {
    yc-prod    = ["2a0d:d6c0:2:ba::/80"],
    yc-preprod = ["2a0d:d6c0:2:ba:ffff::/80"],
    israel     = [], # TODO
    nemax      = [], # WTF?
  }
}
