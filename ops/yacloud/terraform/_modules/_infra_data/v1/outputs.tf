output "k8s_cluster" {
  value = data.yandex_kubernetes_cluster.this
}

output "k8s_endpoint" {
  value = "https://${local.k8s_address}"
}

output "secgroup_http_from_yandex_only" {
  value = data.yandex_vpc_security_group.http_from_yandex_only
}

output "secgroup_allow_all" {
  value = data.yandex_vpc_security_group.allow_all
}

output "internal_cert" {
  value = data.local_file.internal_cert.content
}

output "internal_cert_path" {
  value = local.internal_cert_path
}
