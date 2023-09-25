locals {
  env_name = "israel"
}

module "constants" {
  source   = "../../_modules/_constants/v1"
  env_name = local.env_name
}

module "subinfra_data" {
  source   = "../../_modules/_subinfra_data/v1"
  env_name = local.env_name
}

provider "ycp" {
  token                = var.iam_token
  cloud_id             = module.constants.env_data.cloud_id
  folder_id            = module.constants.env_data.folder_id
  ycp_profile          = module.constants.env_data.ycp_profile
  environment          = module.constants.env_data.ycp_environment
  storage_endpoint_url = module.constants.env_data.s3_endpoint
}

provider "yandex" {
  token     = var.iam_token
  cloud_id  = module.constants.env_data.cloud_id
  folder_id = module.constants.env_data.folder_id
  endpoint  = module.constants.env_data.cloud_api_endpoint
}

terraform {
  required_providers {
    ycp = {
      source = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ycp"
    }
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }

  backend "s3" {
    endpoint = "storage.il.nebius.cloud"
    bucket   = "dl-tf-state"
    region   = "us-east-1"
    key      = "datalens-infra.tfstate"

    skip_region_validation      = true
    skip_credentials_validation = true
  }

  required_version = ">= 0.13"
}

module "main" {
  source = "../../_modules/_main_infra/v1"

  env_name   = "israel"
  yc_profile = "israel"

  k8s_version        = "1.24"
  k8s_cluster_region = "il1"

  setup_logbroker = false
  enable_kafka    = true
  logbroker_name  = "NA"

  kafka_settings = {
    disk_size                    = 20
    public_ip                    = false
    brokers_count                = 1
    resource_preset_id           = "s3-c2-m8"
    version                      = "3.2"
    zones                        = sort([for location in module.subinfra_data.locations : location.zone])
    zookeeper_resource_preset_id = "s3-c2-m8"
  }

  providers = {
    yandex = yandex
    ycp    = ycp
  }
}
