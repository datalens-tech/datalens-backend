resource "yandex_iam_service_account" "cluster" {
  name = "k8s-cluster-${var.cluster_name}"
}

resource "yandex_iam_service_account" "node" {
  name = "k8s-node-${var.cluster_name}"
}

resource "yandex_resourcemanager_folder_iam_member" "cluster_editor" {
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.cluster.id}"
  role      = "admin" # "editor"
}

resource "yandex_resourcemanager_folder_iam_member" "node_image_puller" {
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.node.id}"
  role      = "container-registry.images.puller"
}
