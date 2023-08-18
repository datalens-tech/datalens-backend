variable "env_name" {
  type = string
}

variable "disable_autopurge" {
  type    = bool
  default = true
}

variable "cluster_name" {
  type = string
}

variable "network_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "topics" {
  type = object({
    k8s_logs_topic       = string
    parsed_logs_topic    = string
    filtered_logs_topic  = string
    usage_tracking_topic = string
    unparsed_logs_topic  = string
  })
}

variable "secret_id" {
  type = string
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
}

variable "users" {
  type = object({
    vector   = string
    transfer = string
  })
}
