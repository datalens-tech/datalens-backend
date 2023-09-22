resource "yandex_logging_group" "alb-logging-group" {
  name             = "alb-logging-group"
  folder_id        = var.folder_id
  retention_period = "72h0m0s"
  #  data_stream      = ycp_ydb_topic.logs-stream.name
}
