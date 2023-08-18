locals {
  env_name = "yc-preprod"
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
  token       = var.iam_token
  cloud_id    = module.constants.env_data.cloud_id
  folder_id   = module.constants.env_data.folder_id
  ycp_profile = module.constants.env_data.ycp_profile
  environment = module.constants.env_data.ycp_environment
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
    endpoint = "storage.cloud-preprod.yandex.net"
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

  env_name = local.env_name

  k8s_version        = "1.22"
  k8s_cluster_region = "ru-central1"

  yc_profile = local.env_name

  logbroker_name = "yc-logbroker-preprod"
  enable_kafka   = true

  providers = {
    yandex = yandex
    ycp    = ycp
  }

  kafka_settings = {
    disk_size                    = 20
    public_ip                    = false
    brokers_count                = 1
    resource_preset_id           = "s2.micro"
    version                      = "2.8"
    zones                        = [module.subinfra_data.locations[1].zone, module.subinfra_data.locations[0].zone] # ru-central1-a ru-central1-b (locations: b, a, c)
    zookeeper_resource_preset_id = "s2.micro"
  }
}
