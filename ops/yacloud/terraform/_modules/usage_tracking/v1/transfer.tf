resource "yandex_datatransfer_endpoint" "clickhouse_usage_tracking_target" {
  name = "clickhouse-usage-tracking"
  settings {
    clickhouse_target {
      cleanup_policy = "CLICKHOUSE_CLEANUP_POLICY_DISABLED"
      alt_names {
        from_name = var.kafka_properties.usage_tracking_topic
        to_name   = local.clickhouse_table
      }
      connection {
        connection_options {
          mdb_cluster_id = var.logs_clickhouse.id
          database       = local.clickhouse_database
          user           = local.clickhouse_transfer_user
          password {
            raw = local.clickhouse_transfer_password
          }
        }
      }
    }
  }

  depends_on = [var.logs_clickhouse]
}

resource "ycp_datatransfer_endpoint" "kafka_usage_tracking_source" {
  name = "kafka-usage-tracking"

  settings {
    kafka_source {
      topic_name = var.kafka_properties.usage_tracking_topic
      auth {
        sasl {
          mechanism = "KAFKA_MECHANISM_SHA512"
          user      = var.kafka_properties.transfer_user
          password { raw = local.kafka_user_transfer_password }
        }
      }
      connection {
        cluster_id = var.kafka_properties.cluster_id
      }
      converter {
        format = "JSON"
        data_schema {
          fields {
            fields {
              name = "event_time"
              type = "DATETIME"
            }
            fields {
              name = "event_date"
              type = "DATETIME"
            }
            fields {
              name = "source_entry_id"
              type = "STRING"
            }
            fields {
              name = "dash_id"
              type = "STRING"
            }
            fields {
              name = "dash_tab_id"
              type = "STRING"
            }
            fields {
              name = "chart_id"
              type = "STRING"
            }
            fields {
              name = "chart_kind"
              type = "STRING"
            }
            fields {
              name = "response_status_code"
              type = "UINT64"
            }
            fields {
              name = "dataset_id"
              type = "STRING"
            }
            fields {
              name = "user_id"
              type = "STRING"
            }
            fields {
              name = "request_id"
              type = "STRING"
            }
            fields {
              name = "folder_id"
              type = "STRING"
            }
            fields {
              name = "query"
              type = "STRING"
            }
            fields {
              name = "source"
              type = "STRING"
            }
            fields {
              name = "connection_id"
              type = "STRING"
            }
            fields {
              name = "dataset_mode"
              type = "STRING"
            }
            fields {
              name = "username"
              type = "STRING"
            }
            fields {
              name = "execution_time"
              type = "INT64"
            }
            fields {
              name = "status"
              type = "STRING"
            }
            fields {
              name = "error"
              type = "STRING"
            }
            fields {
              name = "connection_type"
              type = "STRING"
            }
            fields {
              name = "host"
              type = "STRING"
            }
            fields {
              name = "cluster"
              type = "STRING"
            }
            fields {
              name = "clique_alias"
              type = "STRING"
            }
            fields {
              name = "cache_used"
              type = "INT8"
            }
            fields {
              name = "cache_full_hit"
              type = "INT8"
            }
            fields {
              name = "is_public"
              type = "UINT8"
            }
            fields {
              name = "endpoint_code"
              type = "STRING"
            }
            fields {
              name = "query_type"
              type = "STRING"
            }
            fields {
              name = "err_code"
              type = "STRING"
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
    null_resource.usage-tracking-table,
  ]
}

resource "yandex_datatransfer_transfer" "usage_tracking" {
  name      = "usage-tracking"
  target_id = yandex_datatransfer_endpoint.clickhouse_usage_tracking_target.id
  source_id = ycp_datatransfer_endpoint.kafka_usage_tracking_source.id
  type      = "INCREMENT_ONLY"
}
