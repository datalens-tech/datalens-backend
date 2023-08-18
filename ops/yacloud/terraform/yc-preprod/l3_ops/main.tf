locals {
  env_name = "yc-preprod"
  # https://juggler.yandex-team.ru/project/datalens_yc-preprod/dashboard/?project=datalens_yc-preprod
  # https://monitoring.yandex-team.ru/projects/datalens_yc-preprod/info
  juggler_project  = "datalens_${local.env_name}"
  solomon_channel  = "juggler-backend"
  solomon_project  = "aoee4gvsepbo0ah4i2j6"
  solomon_endpoint = "https://solomon.cloud-preprod.yandex-team.ru"
  telegram_chat    = "datalens_preprod_alerts"
}

module "constants" {
  source   = "../../_modules/_constants/v1"
  env_name = local.env_name
}

module "subinfra_data" {
  source   = "../../_modules/_subinfra_data/v1"
  env_name = local.env_name
}

module "main" {
  source = "../../_modules/_main_ops/v1"

  iam_token = var.iam_token

  env_name         = local.env_name
  solomon_endpoint = local.solomon_endpoint
  solomon_channel  = local.solomon_channel
  solomon_project  = local.solomon_project
  juggler_project  = local.juggler_project
  juggler_env_tag  = "datalens-yc-preprod"
  telegram_chat    = local.telegram_chat

  file_connector_bucket_names = [
    module.constants.file_connector_persistent_bucket_name,
    module.constants.file_connector_tmp_bucket_name,
  ]

  alb_5xx_alert = {
    threshold_alarm = 0.05
    threshold_error = 0.15
  }

  alb_4xx_velocity_alert = {
    enabled         = true
    threshold_alarm = 0.10
    threshold_error = 0.20
  }

  enable_logs           = true
  enable_usage_tracking = true

  # backend_group == backend_group id
  app_info = [
    {
      name          = "dataset-data-api"
      backend_group = "bg-e2e3b6f08d447f483544d11a5770e137a77fdd5b-30385"
    },
    {
      name          = "public-data-api"
      backend_group = "bg-23bc1c4d3eb4cc3a1c37c110b25d2e1a7b729457-30918"
    },
    {
      name          = "dls"
      backend_group = "bg-7fb39b2bfbabeefceb1f905fad4459532c0906cc-31890"
    },
    {
      name          = "file-uploader-internal-api"
      backend_group = "bg-a8320079c14765bc93ff7e62ef5ad492e4762ac7-31442"
    },
    {
      name          = "dataset-api"
      backend_group = "bg-4320cbe5d3415b5d18fa133f2014c4b26ae6569e-31268"
    },
    {
      name          = "file-uploader-uploads"
      backend_group = "bg-3b55815140232d8755242a63062d8dad38a6dc63-31442"
    },
  ]

  clickhouse_logs_settings = {
    name               = "logs"
    disk_size          = 50
    resource_preset_id = "s3-c4-m16"
    # there is some bug
    # https://st.yandex-team.ru/MDB-20290
    cloud_storage = false
    network_id    = module.constants.env_data.network_id
    ch_hosts      = [module.subinfra_data.ops_locations[1]]
    secret_id     = "fc3bai9r30l9pdi78dv6"
  }
  console_base_url = "https://console-preprod.cloud.yandex.ru"

  jaeger_host = "jaeger-collector.private-api.ycp.cloud-preprod.yandex.net:443"

  docker_registry_repo = "cr.cloud-preprod.yandex.net/crta0bkj59h30lbm28vu"

  enable_sentry = false

  providers = {
    ytr = ytr
    ycp = ycp
  }
  enable_cloud_logs = true
}

