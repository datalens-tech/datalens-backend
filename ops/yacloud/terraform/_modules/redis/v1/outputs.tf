output "name" {
  value = var.name
}

output "hosts" {
  value = yandex_mdb_redis_cluster.this.host[*].fqdn
}

output "tls_enabled" {
  value = yandex_mdb_redis_cluster.this.tls_enabled
}
