terraform {
  required_providers {
    sentry = {
      source  = "jianyuan/sentry"
      version = "0.10.0"
    }
    yandex = {
      source = "yandex-cloud/yandex"
    }
    ycp = {
      source = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ycp"
    }
  }
  backend "s3" {
    endpoint = "storage.nemax.nebius.cloud"
    bucket   = "dl-tf-state"
    region   = "us-east-1"
    key      = "datalens-sentry-projects.tfstate"

    skip_region_validation      = true
    skip_credentials_validation = true
  }
  required_version = ">= 0.13"
}

provider "sentry" {
  token    = var.sentry_token
  base_url = "https://sentry.back.datalens.nemax.nebiuscloud.net/api/"
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
  token            = var.iam_token
  cloud_id         = module.constants.env_data.cloud_id
  folder_id        = module.constants.env_data.folder_id
  endpoint         = module.constants.env_data.cloud_api_endpoint
  storage_endpoint = module.constants.env_data.s3_endpoint
}
