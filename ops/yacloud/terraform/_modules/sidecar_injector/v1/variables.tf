variable "jaeger_collector_netloc" {
  type        = string
  description = "Jaeger collector netloc"
}

variable "internal_cert" {
  type = string
}

variable "apps_namespace" {
  type = string
}

variable "docker_registry_repo" {
  type = string
}

variable "app_tls_port" {
  type = number
}
