output "env_data" {
  value = var.data[var.env_name]
}

output "use_internal_ca" {
  value = var.env_name == "yc-preprod"
}

output "users" {
  value = local.users
}

output "yandex_cidrs" {
  value = local.yandex_cidrs
}

output "k8s_cluster_name" {
  value = "${local.common_prefix}-${var.env_name}"
}

output "secgroup_http_from_yandex_only_name" {
  value = "${local.common_prefix}-http-from-yandex-only"
}

output "secgroup_allow_all_name" {
  value = "${local.common_prefix}-allow-all"
}

output "kafka_users" {
  value = local.kafka_users
}

output "kafka_topics" {
  value = local.kafka_topics
}

output "kafka_cluster_name" {
  value = local.kafka_cluster_name
}

output "logs_clickhouse_users" {
  value = local.logs_clickhouse_users
}

output "enable_silo" {
  value = var.env_name == "yc-preprod"
}

output "app_tls_port" {
  value = local.app_tls_port
}

output "file_connector_tmp_bucket_name" {
  value = "dl-${var.env_name}-fileconnector-tmp"
}

output "file_connector_persistent_bucket_name" {
  value = "dl-${var.env_name}-fileconnector-persistent"
}
