module "infra_data" {
  source   = "../../_infra_data/v1"
  env_name = var.env_name
}

module "constants" {
  source   = "../../_constants/v1"
  env_name = var.env_name
}

locals {
  backend_groups = join(
    "|",
    [for app_info in var.app_info : app_info.backend_group]
  )
}

locals {
  raw_event_alb_5xx_service = "backend_alb_5xx"
  alb_5xx_service           = "backend_alb_5xx"
  alb_5xx                   = "5xx"
  alb_4xx_velocity_service  = "backend_alb_4xx_velocity"
  alb_4xx_velocity          = "4xx_velocity"
  alb_service               = "backend_alb"
}

locals {
  backend_kafka_service                = "backend_kafka"
  backend_kafka_service_active_brokers = "active_brokers"
  backend_kafka_empty_space            = "empty_space"
}

locals {
  app_logs_transfer_service       = "backend_app_logs_transfer"
  app_logs_transfer_rows_pushed   = "rows_pushed"
  app_logs_transfer_rows_parsed   = "rows_parsed"
  app_logs_transfer_rows_unparsed = "rows_unparsed"
}

locals {
  logs_clickhouse_service     = "backend_clickhouse_logs"
  logs_clickhouse_empty_space = "empty_space"
}

locals {
  k8s_endpoint = module.infra_data.k8s_endpoint
  k8s_ca       = module.infra_data.k8s_cluster.master[0].cluster_ca_certificate
}

data "yandex_client_config" "client" {}
