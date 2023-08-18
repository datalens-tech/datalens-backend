terraform {
  required_providers {
    helm = {
      source = "hashicorp/helm"
    }
    yandex = {
      source = "yandex-cloud/yandex"
    }
    ycp = {
      source = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ycp"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
    }
  }
}
