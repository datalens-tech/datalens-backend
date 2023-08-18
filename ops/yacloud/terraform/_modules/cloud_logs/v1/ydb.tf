resource "yandex_ydb_database_serverless" "logs-stream-db" {
  name      = "logs-database"
  folder_id = var.folder_id
}


resource "ycp_ydb_topic" "logs-stream" {

  database_endpoint   = yandex_ydb_database_serverless.logs-stream-db.ydb_full_endpoint
  name                = "logs-stream"
  partitions_count    = 1
  retention_period_ms = 64800 * 1000 # 18h
  supported_codecs    = ["raw"]
}
