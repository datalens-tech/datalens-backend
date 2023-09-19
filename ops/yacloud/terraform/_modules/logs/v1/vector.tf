locals {
  internal_ca             = "internal-ca"
  internal_ca_volume_name = "internal-ca-volume"
  internal_ca_path        = "/etc/internal-certs"
  internal_ca_filename    = "YandexInternalRootCA.crt"
}

locals {
  backend_kafka_vector_setting = {
    version = "0.16.3"
    k8s_ns  = "logging"

    data_dir = "/vector-data-dir"

    config = {
      type = "kafka"
      bootstrap_servers = join(
        ",",
        [for host in var.kafka_cluster.host : "${host.name}:9091"],
      )
      tls = {
        ca_file = "${local.internal_ca_path}/${local.internal_ca_filename}"
        enabled = true
      }
      sasl = {
        enabled   = true
        mechanism = "SCRAM-SHA-512"
        username  = var.kafka_properties.vector_user
        password  = local.kafka_user_vector_password
      }
    }
    extraVolumes = [
      {
        name : local.internal_ca_volume_name
        configMap : {
          name : local.internal_ca
        }
      }
    ]
    extraVolumeMounts = [
      {
        name : local.internal_ca_volume_name
        mountPath : local.internal_ca_path
      }
    ]
  }
}

locals {
  v_k8s_logs_agent_values = {
    role                        = "Agent"
    podValuesChecksumAnnotation = true

    tolerations = [
      {
        key    = "node-role.kubernetes.io/master"
        effect = "NoSchedule"
      },
      // Schedule even if host is dedicated for special needs
      {
        key      = "dedicated"
        operator = "Exists"
        effect   = "NoSchedule"
      },
    ]

    securityContext = {
      allowPrivilegeEscalation = false

      capabilities = {
        drop = ["all"]
        add  = ["dac_override"]
      }
    }

    customConfig = {
      data_dir = local.backend_kafka_vector_setting.data_dir
      api      = { enabled = false }
      sources = {
        k8s_logs         = { type = "kubernetes_logs" }
        internal_metrics = { type = "internal_metrics" }
      }
      sinks = {
        # standard vector chart can't run without prom_exporter
        # chart depends on port value
        prom_exporter = {
          type    = "prometheus_exporter"
          inputs  = ["internal_metrics"]
          address = "0.0.0.0:9090"
        }
        kafka_default = merge(local.backend_kafka_vector_setting.config, {
          encoding = { codec = "json" }
          inputs   = ["k8s_logs"]
          topic    = var.kafka_properties.k8s_logs_topic
        })
      }
    }

    extraVolumes      = local.backend_kafka_vector_setting.extraVolumes
    extraVolumeMounts = local.backend_kafka_vector_setting.extraVolumeMounts
  }

  v_log_parser_values = {
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
      data_dir = local.backend_kafka_vector_setting.data_dir
      api      = { enabled = false }
      sources = {
        internal_metrics = {
          type = "internal_metrics"
        }
        kafka_raw = merge(local.backend_kafka_vector_setting.config, {
          topics   = [var.kafka_properties.k8s_logs_topic]
          group_id = "vector-log-parser"
        })
      }
      transforms = {
        parse_message_field = {
          type   = "remap"
          inputs = ["kafka_raw"]
          source = <<EOT
parsed_event = parse_json!(.message)
parsed_message = parse_json(parsed_event.message) ?? {"msg": .message, "unparsed": true}
. = parsed_message
EOT
        }
        filter_invalid_message_records = {
          type      = "filter"
          inputs    = ["parse_message_field"]
          condition = <<EOT
.unparsed == true || (!exists(.@fields) && !exists(.app_name))
EOT
        }
        filter_valid_message_records = {
          type      = "filter"
          inputs    = ["parse_message_field"]
          condition = <<EOT
.unparsed != true && !exists(.@fields) && exists(.app_name)
EOT
        }
        filter_valid_message_dls_records = {
          type      = "filter"
          inputs    = ["parse_message_field"]
          condition = <<EOT
.unparsed != true && exists(.@fields)
EOT
        }
        parse_app_log = {
          type   = "remap"
          inputs = ["filter_valid_message_records"]
          source = <<EOT
result_event.app_name = .app_name
result_event.datetime = to_timestamp!(.timestamp)
result_event.message = .message
result_event.request_id = .request_id
result_event.app_version = .app_version
result_event.caller_info = to_string!(.name) + ":" + to_string!(.lineno)
result_event.hostname = .hostname
result_event.level_name = .levelname
result_event.name = .name
result_event.pid = .pid
result_event.timestamp = to_int!(.timestamp)

. = result_event
          EOT
        }
        parse_app_dls_log = {
          type   = "remap"
          inputs = ["filter_valid_message_dls_records"]
          source = <<EOT
result_event.app_name = .@fields.app_name
result_event.datetime = to_timestamp!(.@fields.unixtime)
result_event.message = .@fields.message
result_event.request_id = .@fields.request_id
result_event.app_version = .@fields.app_version
result_event.caller_info = .@fields.caller_info
result_event.hostname = .@fields.hostname
result_event.level_name = .@fields.levelname
result_event.name = .@fields.name
result_event.pid = .@fields.pid
result_event.timestamp = .@fields.unixtime

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
        kafka_parsed = merge(local.backend_kafka_vector_setting.config, {
          inputs   = ["filter_valid_message_records", "filter_valid_message_dls_records"]
          topic    = var.kafka_properties.parsed_logs_topic
          encoding = { codec = "json" }
        })
        kafka_filtered = merge(local.backend_kafka_vector_setting.config, {
          inputs   = ["parse_app_log", "parse_app_dls_log"]
          topic    = var.kafka_properties.filtered_logs_topic
          encoding = { codec = "json" }
        })
        kafka_unparsed = merge(local.backend_kafka_vector_setting.config, {
          inputs   = ["filter_invalid_message_records"]
          topic    = var.kafka_properties.unparsed_logs_topic
          encoding = { codec = "json" }
        })
      }
    }

    extraVolumes      = local.backend_kafka_vector_setting.extraVolumes
    extraVolumeMounts = local.backend_kafka_vector_setting.extraVolumeMounts
  }
}

resource "helm_release" "vector-raw-logs" {
  name       = "vector-raw-logs"
  repository = "https://helm.vector.dev"
  chart      = "vector"
  version    = local.backend_kafka_vector_setting.version

  namespace        = local.backend_kafka_vector_setting.k8s_ns
  create_namespace = false
  cleanup_on_fail  = true
  wait_for_jobs    = true
  atomic           = true

  depends_on = [
    kubernetes_namespace.vector_ns,
    kubernetes_config_map.internal_ca,
    var.kafka_cluster,
  ]

  values = [
    yamlencode(local.v_k8s_logs_agent_values)
  ]
}

resource "helm_release" "vector-log-parser" {
  name       = "vector-log-parser"
  repository = "https://helm.vector.dev"
  chart      = "vector"
  version    = local.backend_kafka_vector_setting.version

  namespace        = local.backend_kafka_vector_setting.k8s_ns
  create_namespace = false
  cleanup_on_fail  = true
  wait_for_jobs    = true
  atomic           = true

  depends_on = [
    kubernetes_namespace.vector_ns,
    kubernetes_config_map.internal_ca,
    var.kafka_cluster,
  ]

  values = [
    yamlencode(local.v_log_parser_values)
  ]
}

resource "kubernetes_network_policy" "allow-system" {
  count  = module.constants.env_data.k8s_use_cilium ? 1 : 0

  metadata {
    name      = "allow-system"
    namespace = local.backend_kafka_vector_setting.k8s_ns
  }

  spec {
    policy_types = ["Egress"]
    pod_selector {}

    egress {
      ports {
        port     = "53"
        protocol = "UDP"
      }
      ports {
        port     = "443"
        protocol = "TCP"
      }
    }
  }

  depends_on = [
    kubernetes_namespace.vector_ns,
  ]
}

resource "kubernetes_network_policy" "allow-kafka" {
  count  = module.constants.env_data.k8s_use_cilium ? 1 : 0

  metadata {
    name      = "allow-kafka"
    namespace = local.backend_kafka_vector_setting.k8s_ns
  }

  spec {
    policy_types = ["Ingress", "Egress"]
    pod_selector {}

    egress {
      ports {
        port     = "9091"
        protocol = "TCP"
      }
    }

    ingress {
      ports {
        port     = "9091"
        protocol = "TCP"
      }
    }
  }

  depends_on = [
    kubernetes_namespace.vector_ns,
  ]
}

resource "kubernetes_config_map" "internal_ca" {
  metadata {
    name      = local.internal_ca
    namespace = local.backend_kafka_vector_setting.k8s_ns
  }
  data = {
    (local.internal_ca_filename) = var.internal_cert
  }
  depends_on = [
    kubernetes_namespace.vector_ns,
  ]
}

resource "kubernetes_namespace" "vector_ns" {
  metadata {
    name = local.backend_kafka_vector_setting.k8s_ns
  }
}
