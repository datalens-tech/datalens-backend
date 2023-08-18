resource "yandex_resourcemanager_folder" "integration-tests" {
  cloud_id = var.cloud_id
  name     = "integration-tests"

  lifecycle {
    prevent_destroy = true
  }
}
