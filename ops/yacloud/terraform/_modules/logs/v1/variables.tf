variable "env_name" {
  type = string
}

variable "clickhouse_settings" {
  type = object({
    name               = string
    disk_size          = number
    resource_preset_id = string
    cloud_storage      = bool
    network_id         = string
    ch_hosts = list(object({
      zone      = string
      subnet_id = string
    }))
  })
}

variable "clickhouse_properties" {
  type = object({
    transfer_user = string
    secret_id     = string
  })
}

variable "enable_usage_tracking" {
  type = bool
}

variable "kafka_cluster" {
  type = object({
    host       = list(any),
    id         = string,
    labels     = map(any),
    name       = string,
    network_id = string,
  })
}

variable "kafka_properties" {
  type = object({
    secret_id           = string
    vector_user         = string
    transfer_user       = string
    filtered_logs_topic = string
    parsed_logs_topic   = string
    k8s_logs_topic      = string
    unparsed_logs_topic = string
  })
}

variable "internal_cert" {
  type = string
}

variable "internal_cert_path" {
  type = string
}
