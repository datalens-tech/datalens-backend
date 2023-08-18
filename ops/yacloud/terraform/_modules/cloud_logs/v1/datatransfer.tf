
#resource "ycp_datatransfer_endpoint" "logs-source" {
#  folder_id = var.folder_id
#  name      = "logs-source"
#
#  settings {
#    yds_source {
#      database           = yandex_ydb_database_serverless.logs-stream-db.database_path
#      service_account_id = yandex_iam_service_account.logs-sa.id
#      stream             = "logs-stream"
#      #      parser {
#      #        cloud_logging_parser {}
#      #      }
#    }
#  }
#}

resource "ycp_datatransfer_endpoint" "logs-receiver" {
  name = "logs-receiver"
  lifecycle {
    ignore_changes = [
      settings[0].object_storage_target[0].buffer_interval,
      settings[0].object_storage_target[0].buffer_size,
      settings[0].object_storage_target[0].output_encoding,
      settings[0].object_storage_target[0].bucket_layout,
      settings[0].object_storage_target[0].connection,
    ]
  }
  settings {
    object_storage_target {
      bucket             = yandex_storage_bucket.logs-bucket.bucket
      service_account_id = yandex_iam_service_account.logs-sa.id
      output_format      = "OBJECT_STORAGE_SERIALIZATION_FORMAT_JSON"
    }
  }
}

#resource "ycp_datatransfer_transfer" "logs-transfer" {
#  folder_id = var.folder_id
#  name       = "logs-transfer"
#  source_id  = ycp_datatransfer_endpoint.logs-source.id
#  target_id  = ycp_datatransfer_endpoint.logs-receiver.id
#  type       = "INCREMENT_ONLY"
#
#}




