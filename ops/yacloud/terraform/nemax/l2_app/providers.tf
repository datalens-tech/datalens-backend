terraform {
  required_providers {
    ycp = {
      source  = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ycp"
      version = "0.111.0"
    }
    yandex = {
      source  = "yandex-cloud/yandex"
      version = ">= 0.84.0"
    }
  }
  required_version = ">= 0.73"

  backend "s3" {
    endpoint = "storage.nemax.nebius.cloud"
    bucket   = "dl-tf-state"
    region   = "us-east-1"
    key      = "datalens-app.tfstate"

    skip_region_validation      = true
    skip_credentials_validation = true
  }
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
