resource "yandex_iam_service_account" "this" {
  name        = "eso-sa"
  description = "service account for k8s external-secrets"
}

resource "yandex_iam_service_account_key" "this" {
  service_account_id = yandex_iam_service_account.this.id
}

resource "yandex_resourcemanager_folder_iam_member" "eso_kms" {
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.this.id}"
  role      = "kms.keys.encrypterDecrypter"
}

resource "yandex_resourcemanager_folder_iam_member" "eso_lockbox" {
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.this.id}"
  role      = "lockbox.payloadViewer"
}
