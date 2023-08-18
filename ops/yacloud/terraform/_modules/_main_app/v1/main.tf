terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
    ycp = {
      source = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ycp"
    }
    helm = {
      source = "hashicorp/helm"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
    }
  }
}

locals {
  k8s_endpoint = module.infra_data.k8s_endpoint
  k8s_ca       = module.infra_data.k8s_cluster.master[0].cluster_ca_certificate
}

data "yandex_client_config" "client" {}

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
