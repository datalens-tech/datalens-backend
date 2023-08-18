module "yc_security" {
  count = module.constants.enable_silo ? 1 : 0

  source = "../../yc_security/v1"

  folder_id        = module.constants.env_data.folder_id
  k8s_cluster_name = module.constants.k8s_cluster_name
}
