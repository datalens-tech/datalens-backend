resource "yandex_iam_service_account" "yds_log_sender" {
  name = "k8s-yds-log-sender-${var.k8s_cluster_name}"
}

resource "yandex_resourcemanager_folder_iam_member" "yds_writer" {
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.yds_log_sender.id}"
  role      = "yds.writer"
}

resource "yandex_iam_service_account_static_access_key" "yds_writer_sa_static_key" {
  service_account_id = yandex_iam_service_account.yds_log_sender.id
  description        = "static access key for object storage"
}

resource "kubernetes_namespace" "yc_security_k8s_namespace" {
  metadata {
    name = "yc-security"
  }
}

resource "kubernetes_secret" "yds-writer-secret" {
  metadata {
    name      = "yds-writer-secret"
    namespace = kubernetes_namespace.yc_security_k8s_namespace.metadata[0].name
  }

  data = {
    accessKeyID     = yandex_iam_service_account_static_access_key.yds_writer_sa_static_key.id
    secretAccessKey = yandex_iam_service_account_static_access_key.yds_writer_sa_static_key.secret_key
  }
}
