resource "yandex_iam_service_account" "this" {
  name = "alb-sa"
}

resource "yandex_iam_service_account_key" "this" {
  service_account_id = yandex_iam_service_account.this.id
}

resource "yandex_resourcemanager_folder_iam_member" "alb_editor" {
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.this.id}"
  role      = "alb.editor"
}

resource "yandex_resourcemanager_folder_iam_member" "vpc_public_admin" {
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.this.id}"
  role      = "vpc.publicAdmin"
}

resource "yandex_resourcemanager_folder_iam_member" "cert_downloader" {
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.this.id}"
  role      = "certificate-manager.certificates.downloader"
}

resource "yandex_resourcemanager_folder_iam_member" "compute_viewer" {
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.this.id}"
  role      = "compute.viewer"
}
