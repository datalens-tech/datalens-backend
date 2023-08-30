variable "env_name" {
  type = string
}

variable "solomon_channel" {
  type = string
}

variable "alb_5xx_alert" {
  type = object({
    threshold_alarm = number
    threshold_error = number
  })
}

variable "alb_4xx_velocity_alert" {
  type = object({
    enabled         = bool
    threshold_alarm = number
    threshold_error = number
  })
}

variable "juggler_project" {
  type = string
}

variable "solomon_endpoint" {
  type = string
}

variable "solomon_project" {
  type = string
}

variable "juggler_env_tag" {
  type = string
}

variable "juggler_description" {
  type    = string
  default = <<EOT
  {{#annotations.juggler_description}}{{{annotations.juggler_description}}}{{/annotations.juggler_description}}
  {{#isAlarm}}Since {{{since}}} {{/isAlarm}}{{{url}}}
  EOT
}

variable "iam_token" {
  type = string
}

variable "app_info" {
  type = list(
    object({
      name          = string
      backend_group = string
    })
  )
}

variable "kafka_empty_space_limit" {
  type    = number
  default = 2000000000 # 2 GB
}

variable "clickhouse_empty_space_warn_limit" {
  type    = number
  default = 0.8
}

variable "clickhouse_empty_space_alarm_limit" {
  type    = number
  default = 0.9
}

variable "telegram_chat" {
  type        = string
  description = "telegram chat id"
}

variable "enable_logs" {
  type = bool
}

variable "enable_cloud_logs" {
  type = bool
}

variable "enable_usage_tracking" {
  type = bool
}

variable "enable_sentry" {
  type = bool
}

variable "sentry_version" {
  type    = string
  default = null
}

variable "sentry_pg_config" {
  type = object({
    preset             = string
    security_group_ids = list(string)
    locations = list(object({
      subnet_id = string
      zone      = string
    }))
  })
  default = null
}

variable "sentry_alb_security_group_id" {
  type    = string
  default = null
}

variable "clickhouse_logs_settings" {
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
    secret_id = string
  })
}

variable "console_base_url" {
  type    = string
  default = null
}

variable "jaeger_host" {
  type = string
}

variable "apps_namespace" {
  default = "bi-backend"
}

variable "docker_registry_repo" {
  type = string
}

variable "file_connector_bucket_names" {
  type = list(string)
}

variable "file_connector_alerts_enabled" {
  type    = bool
  default = true
}
