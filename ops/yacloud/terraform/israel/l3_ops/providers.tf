terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "0.89.0"
    }
    ytr = {
      source  = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ytr"
      version = "0.9.0"
    }
    ycp = {
      source  = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ycp"
      version = "0.114.0"
    }
  }
  backend "s3" {
    endpoint = "storage.il.nebius.cloud"
    bucket   = "dl-tf-state"
    region   = "us-east-1"
    key      = "datalens-ops.tfstate"

    skip_region_validation      = true
    skip_credentials_validation = true
  }
  required_version = ">= 0.13"
}

provider "yandex" {
  token            = var.iam_token
  cloud_id         = module.constants.env_data.cloud_id
  folder_id        = module.constants.env_data.folder_id
  endpoint         = module.constants.env_data.cloud_api_endpoint
  storage_endpoint = module.constants.env_data.s3_endpoint
}

provider "ycp" {
  token                = var.iam_token
  cloud_id             = module.constants.env_data.cloud_id
  folder_id            = module.constants.env_data.folder_id
  storage_endpoint_url = module.constants.env_data.s3_endpoint
  ycp_profile          = module.constants.env_data.ycp_profile
  environment          = module.constants.env_data.ycp_environment
}

provider "ytr" {
  solomon_token      = var.iam_token
  solomon_endpoint   = "https://solomon.yandexcloud.co.il"
  solomon_token_type = "Bearer"
  juggler_endpoint   = "https://api.juggler.yandexcloud.co.il"
  juggler_token_type = "Bearer"
  juggler_token      = var.iam_token
}

data "yandex_client_config" "client" {}

provider "helm" {
  kubernetes {
    host                   = module.infra_data.k8s_endpoint
    cluster_ca_certificate = module.infra_data.k8s_cluster.master[0].cluster_ca_certificate
    token                  = data.yandex_client_config.client.iam_token
  }
}

provider "kubernetes" {
  host                   = module.infra_data.k8s_endpoint
  cluster_ca_certificate = module.infra_data.k8s_cluster.master[0].cluster_ca_certificate
  token                  = data.yandex_client_config.client.iam_token
}
