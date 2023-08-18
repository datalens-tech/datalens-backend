locals {
  secrets = var.tf_managed_secrets ? [
    "common", "bi_api", "service_account", "file_uploader", "frozen_connectors", "service_connectors"
  ] : []
}

resource "yandex_lockbox_secret" "app_secret" {
  for_each = toset(local.secrets)

  name                = each.key
  kms_key_id          = module.secrets.kms_key_id
  folder_id           = module.constants.env_data.folder_id
  deletion_protection = true
}
