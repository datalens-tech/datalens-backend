terraform {
  required_providers {
    ytr = {
      source  = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ytr"
      version = "0.9.0"
    }
    ycp = {
      source = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ycp"
    }
    yandex = {
      source = "yandex-cloud/yandex"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
    }
    kubectl = {
      source = "gavinbunney/kubectl"
    }
  }
  required_version = ">= 0.13"
}

provider "helm" {
  kubernetes {
    host                   = local.k8s_endpoint
    cluster_ca_certificate = local.k8s_ca
    token                  = data.yandex_client_config.client.iam_token
  }
}

provider "kubernetes" {
  host                   = local.k8s_endpoint
  cluster_ca_certificate = local.k8s_ca
  token                  = data.yandex_client_config.client.iam_token
}

provider "kubectl" {
  host                   = local.k8s_endpoint
  cluster_ca_certificate = local.k8s_ca
  token                  = data.yandex_client_config.client.iam_token
  load_config_file       = false
}
