resource "yandex_iam_service_account" "logs-sa" {
  folder_id = var.folder_id
  name      = "cloudlogs-sa"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_storage_editor" {
  folder_id = var.folder_id
  role      = "storage.editor"
  member    = "serviceAccount:${yandex_iam_service_account.logs-sa.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_editor" {
  folder_id = var.folder_id
  role      = "editor"
  member    = "serviceAccount:${yandex_iam_service_account.logs-sa.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_kms" {
  count = var.use_s3_encryption ? 1 : 0

  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.logs-sa.id}"
  role      = "kms.keys.encrypterDecrypter"
}

resource "yandex_iam_service_account_static_access_key" "sa_static_key" {
  service_account_id = yandex_iam_service_account.logs-sa.id
  description        = "static access key for object storage"
}
