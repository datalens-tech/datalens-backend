variable "network_id" {
  type = string
}

variable "names" {
  type = object({
    http_from_yandex_only = string
    allow_all             = string
  })
}
