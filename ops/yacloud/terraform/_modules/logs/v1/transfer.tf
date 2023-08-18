resource "yandex_datatransfer_endpoint" "clickhouse_backend_app_logs_target" {
  name = "clickhouse-backend-app-logs"
  settings {
    clickhouse_target {
      cleanup_policy = "CLICKHOUSE_CLEANUP_POLICY_DISABLED"
      alt_names {
        from_name = var.kafka_properties.filtered_logs_topic
        to_name   = local.clickhouse_backend_app_logs_table
      }
      connection {
        connection_options {
          mdb_cluster_id = yandex_mdb_clickhouse_cluster.log-storage.id
          database       = local.clickhouse_backend_logs_database
          user           = local.clickhouse_transfer_user
          password {
            raw = local.clickhouse_transfer_password
          }
        }
      }
    }
  }

  depends_on = [yandex_mdb_clickhouse_cluster.log-storage]
}

resource "ycp_datatransfer_endpoint" "kafka_backend_app_logs_source" {
  name = "kafka-backend-app-logs"

  settings {
    kafka_source {
      topic_name = var.kafka_properties.filtered_logs_topic
      auth {
        sasl {
          mechanism = "KAFKA_MECHANISM_SHA512"
          user      = var.kafka_properties.transfer_user
          password {
            raw = local.kafka_user_transfer_password
          }
        }
      }
      connection {
        cluster_id = var.kafka_cluster.id
      }
      converter {
        format = "JSON"
        data_schema {
          fields {
            fields {
              name = "request_id"
              type = "STRING"
            }
            fields {
              name = "datetime"
              type = "DATETIME"
            }
            fields {
              name = "message"
              type = "STRING"
            }
            fields {
              name = "app_name"
              type = "STRING"
            }
            fields {
              name = "app_version"
              type = "STRING"
            }
            fields {
              name = "caller_info"
              type = "STRING"
            }
            fields {
              name = "hostname"
              type = "STRING"
            }
            fields {
              name = "pid"
              type = "UINT64"
            }
            fields {
              name = "level_name"
              type = "STRING"
            }
            fields {
              name = "timestamp"
              type = "UINT64"
            }
          }
        }
      }
    }
  }

  # there is a bug in ycp_datatransfer_endpoint
  # provider does not check a lot of fields in response

  lifecycle {
    ignore_changes = [
      settings[0].kafka_source[0].converter,
      settings[0].kafka_source[0].auth[0].sasl,
      settings[0].kafka_source[0].parser
    ]
  }

  depends_on = [
    null_resource.log-storage-app-logs-table,
  ]
}

resource "yandex_datatransfer_transfer" "backend_app_logs" {
  name      = "backend-app-logs"
  target_id = yandex_datatransfer_endpoint.clickhouse_backend_app_logs_target.id
  source_id = ycp_datatransfer_endpoint.kafka_backend_app_logs_source.id
  type      = "INCREMENT_ONLY"
}
