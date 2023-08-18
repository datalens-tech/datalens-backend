variable "env_name" {
  type = string
}

variable "logs_clickhouse" {
  type = object({
    host = list(object({
      fqdn = string
    }))
    id = string
  })
}

variable "kafka_properties" {
  type = object({
    cluster_id           = string
    secret_id            = string
    vector_user          = string
    transfer_user        = string
    usage_tracking_topic = string
    parsed_logs_topic    = string
  })
}

variable "clickhouse_properties" {
  type = object({
    secret_id     = string
    transfer_user = string
  })
}

variable "backend_kafka_vector_setting" {
  type = object({
    version           = string
    k8s_ns            = string
    config            = any
    extraVolumes      = list(any)
    extraVolumeMounts = list(any)
  })
}

variable "vector_internal_ca" {
  type = any
}

variable "vector_kubernetes_namespace" {
  type = any
}

variable "internal_cert_path" {
  type = string
}
