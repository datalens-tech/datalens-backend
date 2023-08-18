data "yandex_mdb_kafka_cluster" "backend_kafka" {
  count = var.enable_logs == true ? 1 : 0
  name  = module.constants.kafka_cluster_name
}

module "logs" {
  count                 = var.enable_logs == true ? 1 : 0
  env_name              = var.env_name
  source                = "../../logs/v1"
  kafka_cluster         = data.yandex_mdb_kafka_cluster.backend_kafka[0]
  enable_usage_tracking = var.enable_usage_tracking
  internal_cert         = module.infra_data.internal_cert
  clickhouse_settings   = var.clickhouse_logs_settings
  internal_cert_path    = module.infra_data.internal_cert_path
  kafka_properties = {
    secret_id           = module.constants.env_data.secret_ids["kafka_passwords"]
    vector_user         = module.constants.kafka_users.kafka_user_vector_name
    transfer_user       = module.constants.kafka_users.kafka_user_transfer_name
    filtered_logs_topic = module.constants.kafka_topics.filtered_logs_topic
    parsed_logs_topic   = module.constants.kafka_topics.parsed_logs_topic
    k8s_logs_topic      = module.constants.kafka_topics.k8s_logs_topic
    unparsed_logs_topic = module.constants.kafka_topics.unparsed_logs_topic
  }
  clickhouse_properties = {
    secret_id     = var.clickhouse_logs_settings.secret_id
    transfer_user = module.constants.logs_clickhouse_users["transfer_user_name"]
  }
}

module "usage_tracking" {
  count                        = var.enable_usage_tracking == true ? 1 : 0
  env_name                     = var.env_name
  source                       = "../../usage_tracking/v1"
  logs_clickhouse              = module.logs[0].logs_clickhouse_cluster
  backend_kafka_vector_setting = module.logs[0].backend_kafka_vector_setting
  vector_internal_ca           = module.logs[0].vector_internal_ca
  vector_kubernetes_namespace  = module.logs[0].vector_kubernetes_namespace
  internal_cert_path           = module.infra_data.internal_cert_path
  kafka_properties = {
    cluster_id           = data.yandex_mdb_kafka_cluster.backend_kafka[0].id
    secret_id            = module.constants.env_data.secret_ids["kafka_passwords"]
    vector_user          = module.constants.kafka_users.kafka_user_vector_name
    transfer_user        = module.constants.kafka_users.kafka_user_transfer_name
    usage_tracking_topic = module.constants.kafka_topics.usage_tracking_topic
    parsed_logs_topic    = module.constants.kafka_topics.parsed_logs_topic
  }
  clickhouse_properties = {
    secret_id     = var.clickhouse_logs_settings.secret_id
    transfer_user = module.constants.logs_clickhouse_users["transfer_user_name"]
  }
}
