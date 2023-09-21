module "k8s" {
  source = "../../k8s/v1"

  yc_profile = var.yc_profile

  k8s_version = var.k8s_version

  service_ipv4_range  = "10.96.0.0/16"
  cluster_ipv4_range  = "10.112.0.0/16"
  cluster_ipv6_range  = "fc00::/96"
  service_ipv6_range  = "fc01::/112"
  security_groups_ids = []
  cluster_name        = module.constants.k8s_cluster_name
  folder_id           = module.constants.env_data.folder_id
  network_id          = module.constants.env_data.network_id
  locations           = module.subinfra_data.locations
  cluster_region      = var.k8s_cluster_region
  use_ext_v6          = module.constants.env_data.k8s_use_ext_v6
  enable_silo         = module.constants.enable_silo
  use_cilium          = module.constants.env_data.k8s_use_cilium

  healthchecks_cidrs = {
    v4 = local.v4_healthchecks_cidrs,
    v6 = local.v6_healthchecks_cidrs[var.env_name]
  }
  yandex_nets = module.constants.yandex_cidrs

  v4_cidrs = module.subinfra_data.v4_cidrs

  bastion = {
    enable          = module.constants.env_data.k8s_use_bastion
    cidr            = module.constants.env_data.bastion_cidr
    endpoint_suffix = module.constants.env_data.bastion_endpoint_suffix
  }

  k8s_audit_security_stream = module.constants.env_data.k8s_audit_security_stream
  alb_security_group_id     = yandex_vpc_security_group.allow_all.id

  providers = {
    yandex = yandex
  }
}

module "bastion" {
  count  = module.constants.env_data.k8s_use_bastion ? 1 : 0
  source = "../../k8s-bastion/v1"

  cluster_id      = module.k8s.cluster_id
  environment     = var.yc_profile
  bastion_enabled = module.constants.env_data.k8s_use_bastion
}
