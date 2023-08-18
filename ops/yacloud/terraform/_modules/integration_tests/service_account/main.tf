resource "yandex_iam_service_account" "integration-tests-sa" {
  name               = "integration-tests-${var.sa_name}"
  folder_id          = var.folder_id
  description        = "DataLens backend integration tests service account"

  lifecycle {
    prevent_destroy = true
  }
}

resource "yandex_iam_service_account_key" "integration-tests-sa-key" {
  service_account_id = yandex_iam_service_account.integration-tests-sa.id
}

resource "yandex_resourcemanager_folder_iam_member" "integration-tests-sa-role" {
  for_each = toset(var.folder_roles)

  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.integration-tests-sa.id}"
  role      = each.key

  lifecycle {
    prevent_destroy = true
  }
}
