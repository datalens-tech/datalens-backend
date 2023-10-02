output "api_config_hash" {
  value = md5(data.local_file.api_config.content)
  depends_on = [kubernetes_config_map.app_configs_v1]
}

output "file_uploader_worker_hash" {
  value = md5(data.local_file.file_uploader_worker_config.content)
  depends_on = [kubernetes_config_map.app_configs_v1]
}

output "file_uploader_api_config_hash" {
  value = md5(data.local_file.file_uploader_api_config.content)
  depends_on = [kubernetes_config_map.app_configs_v1]
}
