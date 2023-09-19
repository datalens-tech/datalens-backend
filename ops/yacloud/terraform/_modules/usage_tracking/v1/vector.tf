locals {
  vector_values = {
    role                        = "Stateless-Aggregator"
    podValuesChecksumAnnotation = true
    podAnnotations              = { "prometheus.io/scrape" = "true" }

    securityContext = {
      allowPrivilegeEscalation = false
      runAsNonRoot             = true
      runAsUser                = 1000

      capabilities = {
        drop = ["all"]
      }
    }

    customConfig = {
      data_dir = "/vector-data-dir"
      api      = { enabled = false }
      sources = {
        internal_metrics = {
          type = "internal_metrics"
        }
        parsed_logs = merge(var.backend_kafka_vector_setting.config, {
          topics   = [var.kafka_properties.parsed_logs_topic]
          group_id = "vector-usage-tracking-parser"
          decoding = { codec = "json" }
        })
      }
      transforms = {
        filter_usage_tracking_records = {
          type      = "filter"
          inputs    = ["parsed_logs"]
          condition = <<EOT
.event_code == "profile_db_request"
          EOT
        }
        parse_usage_tracking = {
          type   = "remap"
          inputs = ["filter_usage_tracking_records"]
          source = <<EOT
result_event.event_time = to_timestamp!(.timestamp)
result_event.event_date = format_timestamp!(result_event.event_time, format: "%F")
result_event.source_entry_id = if !is_nullish(.dataset_id) {
  .dataset_id
} else if !is_nullish(.connection_id) {
  .connection_id
} else {
  "__unknown__"
}
result_event.dash_id = .dash_id
result_event.dash_tab_id = .dash_tab_id
result_event.chart_id = .chart_id
result_event.chart_kind = .chart_kind
result_event.response_status_code = .response_status_code
result_event.dataset_id = .dataset_id
result_event.user_id = .user_id
result_event.request_id = .request_id
result_event.folder_id = .billing_folder_id
result_event.query = .query
result_event.source = .source
result_event.connection_id = .connection_id
result_event.dataset_mode = .dataset_mode
result_event.username = .username
result_event.execution_time = .execution_time
result_event.status = .status
result_event.error = .error
result_event.connection_type = .connection_type
result_event.host = .host
result_event.cluster = .cluster
result_event.clique_alias = .clique_alias
result_event.cache_used = .cache_used
result_event.cache_full_hit = .cache_full_hit
result_event.is_public = if !is_nullish(.is_public) {
  .is_public
} else {
  to_int(is_nullish(.user_id))
}
result_event.endpoint_code = .endpoint_code
result_event.query_type = .query_type
result_event.err_code = .err_code

. = result_event
          EOT
        }
      }
      sinks = {
        # standard vector chart can't run without prom_exporter
        # chart depends on port value
        prom_exporter = {
          type    = "prometheus_exporter"
          inputs  = ["internal_metrics"]
          address = "0.0.0.0:9090"
        }
        usage_tracking = merge(var.backend_kafka_vector_setting.config, {
          inputs   = ["parse_usage_tracking"]
          topic    = var.kafka_properties.usage_tracking_topic
          encoding = { codec = "json" }
        })
      }
    }

    extraVolumes      = var.backend_kafka_vector_setting.extraVolumes
    extraVolumeMounts = var.backend_kafka_vector_setting.extraVolumeMounts
  }
}

resource "helm_release" "vector-usage-tracking-parser" {
  name       = "vector-usage-tracking-parser"
  repository = "https://helm.vector.dev"
  chart      = "vector"
  version    = var.backend_kafka_vector_setting.version

  namespace        = var.backend_kafka_vector_setting.k8s_ns
  create_namespace = false
  cleanup_on_fail  = true
  wait_for_jobs    = true
  atomic           = true

  depends_on = [
    var.vector_kubernetes_namespace,
    var.vector_internal_ca,
  ]

  values = [
    yamlencode(local.vector_values)
  ]
}
