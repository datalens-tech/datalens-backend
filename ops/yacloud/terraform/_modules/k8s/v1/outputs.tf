output "cluster_id" {
  value = yandex_kubernetes_cluster.this.id
}

output "endpoint" {
  value = local.endpoint
}

output "ca" {
  value = local.ca
}

output "k8s_ng_sa_id" {
  value = yandex_iam_service_account.node.id
}
