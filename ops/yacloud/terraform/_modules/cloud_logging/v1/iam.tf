resource "yandex_iam_service_account" "this" {
  name = "cloud-logging-writer"
}

resource "yandex_iam_service_account_key" "this" {
  service_account_id = yandex_iam_service_account.this.id
}

resource "yandex_resourcemanager_folder_iam_member" "sa_role" {
  for_each = toset(["logging.writer", "monitoring.editor"])

  role      = each.value
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.this.id}"
}

locals {
  sa_key = {
    id                 = yandex_iam_service_account_key.this.id
    created_at         = yandex_iam_service_account_key.this.created_at
    key_algorithm      = yandex_iam_service_account_key.this.key_algorithm
    public_key         = yandex_iam_service_account_key.this.public_key
    private_key        = yandex_iam_service_account_key.this.private_key
    service_account_id = yandex_iam_service_account_key.this.service_account_id
  }
}
