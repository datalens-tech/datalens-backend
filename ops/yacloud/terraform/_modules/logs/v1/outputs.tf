output "clickhouse_log_storage_id" {
  value = yandex_mdb_clickhouse_cluster.log-storage.id
}

output "logs_clickhouse_cluster" {
  value = yandex_mdb_clickhouse_cluster.log-storage
}

output "backend_kafka_vector_setting" {
  value = local.backend_kafka_vector_setting
}

output "backend_app_logs_transfer_id" {
  value = yandex_datatransfer_transfer.backend_app_logs.id
}

output "vector_internal_ca" {
  value = kubernetes_config_map.internal_ca
}

output "vector_kubernetes_namespace" {
  value = kubernetes_namespace.vector_ns
}
