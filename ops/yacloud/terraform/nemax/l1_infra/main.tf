locals {
  env_name = "nemax"
}

module "constants" {
  source   = "../../_modules/_constants/v1"
  env_name = local.env_name
}

module "subinfra_data" {
  source   = "../../_modules/_subinfra_data/v1"
  env_name = local.env_name
}

module "security_groups" {
  source = "../../_modules/security_groups/v1"

  network_id = module.constants.env_data.network_id
  names = {
    http_from_yandex_only = module.constants.secgroup_http_from_yandex_only_name
    allow_all             = module.constants.secgroup_allow_all_name
  }
}

module "k8s" {
  source = "../../_modules/k8s/v1"

  yc_profile = local.env_name

  k8s_version = "1.24"

  service_ipv4_range  = "10.96.0.0/16"
  cluster_ipv4_range  = "10.112.0.0/16"
  cluster_ipv6_range  = "fc00::/96"
  service_ipv6_range  = "fc01::/112"
  security_groups_ids = []
  cluster_name        = module.constants.k8s_cluster_name
  folder_id           = module.constants.env_data.folder_id
  network_id          = module.constants.env_data.network_id
  locations           = module.subinfra_data.locations
  cluster_region      = "eu-north1"
  use_ext_v6          = module.constants.env_data.k8s_use_ext_v6
  enable_silo         = module.constants.enable_silo
  use_cilium          = module.constants.env_data.k8s_use_cilium

  healthchecks_cidrs = {
    v4 = [
      "198.18.235.0/24",
      "198.18.248.0/24",
    ],
    v6 = []
  }
  yandex_nets = module.constants.yandex_cidrs

  v4_cidrs = module.subinfra_data.v4_cidrs

  bastion = {
    enable          = false
    cidr            = null
    endpoint_suffix = null
  }

  providers = {
    yandex = yandex
  }
}
