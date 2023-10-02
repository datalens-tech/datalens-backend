data "local_file" "api_config" {
  filename = "configs/v1/api.yaml"
}

data "local_file" "file_uploader_api_config" {
  filename = "configs/v1/file_uploader_api.yaml"
}

data "local_file" "file_uploader_worker_config" {
  filename = "configs/v1/file_uploader_worker.yaml"
}

resource "kubernetes_config_map" "app_configs_v1" {
  metadata {
    name      = "app-configs-v1"
    namespace = var.k8s_namespace
  }
  data = {
    "api.yaml"                  = data.local_file.api_config.content
    "file_uploader_api.yaml"    = data.local_file.file_uploader_api_config.content
    "file_uploader_worker.yaml" = data.local_file.file_uploader_worker_config.content
  }
}
