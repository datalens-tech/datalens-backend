resource "yandex_iam_service_account" "sa" {
  folder_id = var.folder_id
  name      = "file-connector-sa"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_editor" {
  folder_id = var.folder_id
  role      = "storage.editor"
  member    = "serviceAccount:${yandex_iam_service_account.sa.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_kms" {
  count = var.use_s3_encryption ? 1 : 0

  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.sa.id}"
  role      = "kms.keys.encrypterDecrypter"
}

resource "yandex_iam_service_account_static_access_key" "sa_static_key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "static access key for object storage"
}

resource "yandex_lockbox_secret" "file_conn_sa_key" {
  folder_id = var.folder_id
  name      = "file_conn_sa_key"
}

resource "yandex_lockbox_secret_version" "file_conn_sa_key_version" {
  secret_id = yandex_lockbox_secret.file_conn_sa_key.id
  entries {
    key        = "access_key"
    text_value = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  }
  entries {
    key        = "secret_key"
    text_value = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
  }
}
