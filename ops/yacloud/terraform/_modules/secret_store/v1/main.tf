terraform {
  required_providers {
    helm = {
      source = "hashicorp/helm"
    }
  }
}

module "infra_data" {
  source = "../../_infra_data/v1"

  env_name = var.env_name
}
