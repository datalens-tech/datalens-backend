module "kafka" {
  count          = var.enable_kafka == true ? 1 : 0
  env_name       = var.env_name
  source         = "../../kafka/v1"
  kafka_settings = var.kafka_settings

  network_id = module.constants.env_data.network_id
  subnet_ids = module.constants.env_data.subnet_ids

  topics = {
    k8s_logs_topic       = module.constants.kafka_topics.k8s_logs_topic
    parsed_logs_topic    = module.constants.kafka_topics.parsed_logs_topic
    filtered_logs_topic  = module.constants.kafka_topics.filtered_logs_topic
    usage_tracking_topic = module.constants.kafka_topics.usage_tracking_topic
    unparsed_logs_topic  = module.constants.kafka_topics.unparsed_logs_topic
  }

  cluster_name = module.constants.kafka_cluster_name
  users = {
    transfer = module.constants.kafka_users.kafka_user_transfer_name
    vector   = module.constants.kafka_users.kafka_user_vector_name
  }
  secret_id = module.constants.env_data.secret_ids["kafka_passwords"]

}
