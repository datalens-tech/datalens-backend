module "cloud-logs" {
  count     = var.enable_cloud_logs == true ? 1 : 0
  source    = "../../cloud_logs/v1"
  folder_id = module.constants.env_data.folder_id
}
