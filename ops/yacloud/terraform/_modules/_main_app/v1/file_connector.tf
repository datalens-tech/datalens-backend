module "file_connector" {
  count  = module.constants.env_data.apps_to_run.file_uploader ? 1 : 0
  source = "../../file_connector/v1"

  env_name   = module.constants.env_data.env_name
  folder_id  = module.constants.env_data.folder_id
  network_id = module.constants.env_data.network_id

  ch_config         = var.file_connector_config.ch_config
  use_s3_encryption = var.file_connector_config.use_s3_encryption

  persistent_bucket_size = var.file_connector_config.persistent_bucket_size
  tmp_bucket_size        = var.file_connector_config.tmp_bucket_size

  disable_autopurge = module.constants.env_data.env_name == "yc-preprod"
}
