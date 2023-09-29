locals {
  env_name        = "nemax"
  juggler_project = "yc.datalens.backend-service-cloud"
  # host field in event
  solomon_channel = "juggler-backend"
  solomon_project = "yc.datalens.backend-service-cloud"
  telegram_chat   = "datalens_backend_nemax_alerts"
  juggler_env_tag = "datalens-yc-nemax"
}

module "constants" {
  source   = "../../_modules/_constants/v1"
  env_name = local.env_name
}

module "subinfra_data" {
  source   = "../../_modules/_subinfra_data/v1"
  env_name = local.env_name
}

module "infra_data" {
  source   = "../../_modules/_infra_data/v1"
  env_name = local.env_name
}

module "main" {
  source = "../../_modules/_main_ops/v1"

  iam_token = var.iam_token

  env_name         = local.env_name
  solomon_channel  = local.solomon_channel
  solomon_project  = local.solomon_project
  solomon_endpoint = "https://solomon.nemax.nebiuscloud.net"
  juggler_project  = local.juggler_project
  telegram_chat    = local.telegram_chat
  juggler_env_tag  = local.juggler_env_tag

  enable_logs           = false
  enable_usage_tracking = false

  clickhouse_logs_settings = {
    name               = "log_storage"
    disk_size          = 50
    resource_preset_id = "s3-c2-m8"
    # special ops network
    # https://console.il.nebius.com/folders/yc.datalens.backend-service-folder/vpc/network/ccm1n0bt0jsbutofq91f/overview
    network_id = module.constants.env_data.ops_network_id
    # there is some bug
    # https://st.yandex-team.ru/MDB-20290
    cloud_storage = false
    ch_hosts = [
      module.subinfra_data.ops_locations[1],
      module.subinfra_data.ops_locations[2]
    ],
    secret_id = "bcnj190e8n80imo45dk0"
    version   = "22.8"
  }

  enable_sentry = true

  alb_5xx_alert = {
    threshold_alarm = 0.05
    threshold_error = 0.15
  }
  alb_4xx_velocity_alert = {
    enabled         = false
    threshold_alarm = 0.10
    threshold_error = 0.20
  }

  # backend_group == backend_group id
  # it works not like in yacloud
  app_info = [
    {
      name          = "dataset-data-api"
      backend_group = "fisgg9sbil577v6tgghm"
    },
    {
      name          = "dataset-api"
      backend_group = "fis9hc5lef91mpfb7veo"
    },
  ]

  sentry_version               = "20.0.0"
  sentry_alb_security_group_id = module.infra_data.secgroup_allow_all.id
  sentry_pg_config = {
    preset             = "c3-c2-m4"
    locations          = [module.subinfra_data.locations[module.constants.env_data.main_zone_idx]]
    security_group_ids = [module.infra_data.secgroup_allow_all.id]
  }

  jaeger_host = "collector.tracing.nemax.nebiuscloud.net:14250"

  docker_registry_repo = " cr.nemax.nebius.cloud/crnct6nddg9f0oicma93"

  file_connector_alerts_enabled = false
  file_connector_bucket_names = [
    module.constants.file_connector_persistent_bucket_name,
    module.constants.file_connector_tmp_bucket_name,
  ]

  providers = {
    ytr    = ytr
    ycp    = ycp
    yandex = yandex
  }
  enable_cloud_logs = false
}

module "cloud_logging" {
  source = "../../_modules/cloud_logging/v1"

  folder_id          = module.constants.env_data.folder_id
  k8s_cluster_id     = module.infra_data.k8s_cluster.id
  cloud_api_endpoint = module.constants.env_data.cloud_api_endpoint
}
