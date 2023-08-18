module "constants" {
  source = "../../_constants/v1"

  env_name = var.env_name
}

module "infra_data" {
  source = "../../_infra_data/v1"

  env_name = var.env_name
}

module "subinfra_data" {
  source = "../../_subinfra_data/v1"

  env_name = var.env_name
}
