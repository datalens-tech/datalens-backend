locals {
  solomon_project = module.constants.env_data.cloud_id
  solomon_cluster = module.constants.env_data.folder_id
  solomon_channels = [
    {
      id = var.solomon_channel,
      config = {
        notifyAboutStatuses = [
          "ERROR",
          "WARN",
          "ALARM",
          "OK"
        ],
        repeatDelaySecs : 0
      }
    }
  ]
  solomon_language_style = "DEFAULT"

  k8s_service_names = setunion(
    module.constants.env_data.apps_to_run.bi_api ? ["dataset-api", "dataset-data-api"] : [],
    module.constants.env_data.apps_to_run.bi_api_public ? ["public-dataset-api"] : [],
    module.constants.env_data.apps_to_run.file_uploader ? ["file-uploader-api", "file-uploader-worker"] : [],
    module.constants.env_data.apps_to_run.secure_reader ? ["file-secure-reader"] : [],
    module.constants.env_data.apps_to_run.dls ? ["dls-api", "dls-tasks"] : [],
  )
  k8s_monitoring_description = var.console_base_url == null ? "" : format(
    " Monitoring: %s/folders/%s/managed-kubernetes/cluster/%s/pod?id=%s&tab=monitoring.",
    var.console_base_url,
    module.constants.env_data.folder_id,
    module.infra_data.k8s_cluster.id,
    "{{labels.namespace}}%3A{{labels.pod}}"
  )
  container_cpu_usage              = "container_cpu_usage"
  container_cpu_usage_alert_window = 600
  container_cpu_usage_delay        = 30
  container_cpu_usage_threshold    = 0.95

  container_mem_usage              = "container_mem_usage"
  container_mem_usage_alert_window = 600
  container_mem_usage_delay        = 30
  container_mem_usage_threshold    = 0.95

  container_restarts              = "container_restarts"
  container_restarts_alert_window = 600
  container_restarts_delay        = 30

  redis_service = "managed-redis"

  redis_is_alive              = "redis_is_alive"
  redis_is_alive_alert_window = 60
  redis_is_alive_delay        = 5

  redis_cpu_usage              = "redis_cpu_usage"
  redis_cpu_usage_alert_window = 600
  redis_cpu_usage_delay        = 30
  redis_cpu_usage_threshold    = 0.9

  redis_mem_usage              = "redis_mem_usage"
  redis_mem_usage_alert_window = 600
  redis_mem_usage_delay        = 30
  redis_mem_usage_threshold    = 0.9

  redis_disk_usage              = "redis_disk_usage"
  redis_disk_usage_alert_window = 600
  redis_disk_usage_delay        = 30
  redis_disk_usage_threshold    = 0.9

  postgresql_service = "managed-postgresql"

  postgresql_is_alive              = "pg_is_alive"
  postgresql_is_alive_alert_window = 60
  postgresql_is_alive_delay        = 5

  postgresql_pooler_is_alive              = "pg_pooler_is_alive"
  postgresql_pooler_is_alive_alert_window = 60
  postgresql_pooler_is_alive_delay        = 5

  postgresql_cpu_usage              = "pg_cpu_usage"
  postgresql_cpu_usage_alert_window = 600
  postgresql_cpu_usage_delay        = 30
  postgresql_cpu_usage_threshold    = 0.9

  postgresql_mem_usage              = "pg_mem_usage"
  postgresql_mem_usage_alert_window = 600
  postgresql_mem_usage_delay        = 30
  postgresql_mem_usage_threshold    = 0.9

  postgresql_disk_usage              = "pg_disk_usage"
  postgresql_disk_usage_alert_window = 600
  postgresql_disk_usage_delay        = 30
  postgresql_disk_usage_threshold    = 0.9

  postgresql_replication_lag              = "pg_replication_lag"
  postgresql_replication_lag_alert_window = 60
  postgresql_replication_lag_delay        = 5
  postgresql_replication_lag_threshold    = 1 # seconds

  clickhouse_service = "managed-clickhouse"

  clickhouse_is_alive              = "ch_is_alive"
  clickhouse_is_alive_alert_window = 60
  clickhouse_is_alive_delay        = 5

  clickhouse_cpu_usage              = "ch_cpu_usage"
  clickhouse_cpu_usage_alert_window = 600
  clickhouse_cpu_usage_delay        = 30
  clickhouse_cpu_usage_threshold    = 0.9

  clickhouse_mem_usage              = "ch_mem_usage"
  clickhouse_mem_usage_alert_window = 600
  clickhouse_mem_usage_delay        = 30
  clickhouse_mem_usage_threshold    = 0.9

  clickhouse_disk_usage              = "ch_disk_usage"
  clickhouse_disk_usage_alert_window = 600
  clickhouse_disk_usage_delay        = 30
  clickhouse_disk_usage_threshold    = 0.9

  storage_service = "storage"

  file_storage_space_usage              = "file_s3_space_usage"
  file_storage_space_usage_alert_window = 600
  file_storage_space_usage_delay        = 30
  file_storage_space_usage_threshold    = 0.85
}

resource "null_resource" "back_channel_to_juggler" {

  provisioner "local-exec" {
    command = "${path.module}/create_channel.sh"
    environment = {
      IAM_TOKEN           = var.iam_token
      SOLOMON_ENDPOINT    = var.solomon_endpoint
      SOLOMON_CHANNEL     = var.solomon_channel
      SOLOMON_PROJECT     = var.solomon_project
      JUGGLER_ENV_TAG     = var.juggler_env_tag
      JUGGLER_DESCRIPTION = var.juggler_description
    }
  }
}

resource "ytr_solomon_alert" "back_alb_5xx" {
  project     = var.solomon_project
  name        = "backend_alb_5xx"
  description = "5xx from application load balancer"
  alert_id    = "backend_alb_5xx"

  raw_json = jsonencode({
    name                = "backend_alb_5xx"
    description         = "5xx from application load balancer"
    noPointsPolicy      = "NO_POINTS_DEFAULT"
    resolvedEmptyPolicy = "RESOLVED_EMPTY_MANUAL"
    severity            = "SEVERITY_UNSPECIFIED"
    groupByLabels = [
      "backend_group",
    ]

    type : {
      expression : {
        checkExpression : "",
        program : format(<<-EOT
          let requests_count_per_second = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="application-load-balancer",
            name="load_balancer.requests_count_per_second",
            code="total",
            backend_group="${local.backend_groups}"
          };
          let errors_count_per_second = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="application-load-balancer",
            name="load_balancer.requests_count_per_second",
            code="5*",
            backend_group="${local.backend_groups}"
          };

          no_data_if(size(requests_count_per_second) == 0);

          let sum_total = sum(group_lines('sum', requests_count_per_second));
          let sum_errors = sum(group_lines('sum', errors_count_per_second));
          let ratio = sum_errors / sum_total;

          let backend_group = get_label(requests_count_per_second, 'backend_group');

          let app_name = 'unknown';
          %s

          alarm_if(ratio >= ${var.alb_5xx_alert.threshold_alarm});
          warn_if(ratio >= ${var.alb_5xx_alert.threshold_alarm});
        EOT
          , join("\n", [for app_info in var.app_info : format("let app_name = backend_group == '%s' ? '%s' : app_name;", app_info.backend_group, app_info.name)])
        )
      }
    }

    channels      = local.solomon_channels
    languageStyle = local.solomon_language_style

    annotations = {
      host = "{{expression.app_name}}"
      # we use common juggler.yandex-team installation
      # so we must add environment prefix to raw event
      service          = "${var.env_name}_${local.alb_5xx_service}"
      backend_group_id = "{{labels.backend_group_id}}"
      error_count      = "{{expression.error_count}}"
    }

    windowSecs = 300
    delaySecs  = 0
  })
}

resource "ytr_solomon_alert" "back_alb_4xx_velocity" {
  count = var.alb_4xx_velocity_alert.enabled ? 1 : 0

  project     = var.solomon_project
  name        = "backend_alb_4xx_velocity"
  description = "4xx velocity from application load balancer"
  alert_id    = "backend_alb_4xx_velocity"

  raw_json = jsonencode({
    name                = "backend_alb_4xx_velocity"
    description         = "4xx velocity from application load balancer"
    noPointsPolicy      = "NO_POINTS_DEFAULT"
    resolvedEmptyPolicy = "RESOLVED_EMPTY_MANUAL"
    severity            = "SEVERITY_UNSPECIFIED"
    groupByLabels = [
      "backend_group",
    ]

    type : {
      expression : {
        checkExpression : "",
        program : format(<<-EOT
          let requests_count_per_second = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="application-load-balancer",
            name="load_balancer.requests_count_per_second",
            code="total",
            backend_group="${local.backend_groups}"
          };
          let errors_count_per_second = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="application-load-balancer",
            name="load_balancer.requests_count_per_second",
            code="4*",
            backend_group="${local.backend_groups}"
          };

          no_data_if(size(requests_count_per_second) == 0);

          let ratio_diff = diff(group_lines('sum', errors_count_per_second) / group_lines('sum', requests_count_per_second));
          let ratio = sum(ratio_diff);

          let backend_group = get_label(requests_count_per_second, 'backend_group');

          let app_name = 'unknown';
          %s

          alarm_if(ratio >= ${var.alb_4xx_velocity_alert.threshold_alarm});
          warn_if(ratio >= ${var.alb_4xx_velocity_alert.threshold_alarm});
        EOT
          , join("\n", [for app_info in var.app_info : format("let app_name = backend_group == '%s' ? '%s' : app_name;", app_info.backend_group, app_info.name)])
        )
      }
    }

    channels      = local.solomon_channels
    languageStyle = local.solomon_language_style

    annotations = {
      host = "{{expression.app_name}}"
      # we use common juggler.yandex-team installation
      # so we must add environment prefix to raw event
      service          = "${var.env_name}_${local.alb_4xx_velocity_service}"
      backend_group_id = "{{labels.backend_group_id}}"
      error_count      = "{{expression.error_count}}"
    }

    windowSecs = 300
    delaySecs  = 0
  })
}

resource "ytr_solomon_alert" "backend_kafka_active_brokers" {
  count = var.enable_logs ? 1 : 0

  project     = var.solomon_project
  name        = "backend_kafka_active_brokers"
  description = "Backend kafka is alive"
  alert_id    = "backend_kafka_active_brokers"

  raw_json = jsonencode({
    name                = "backend_kafka_active_brokers"
    description         = "Backend kafka is alive"
    noPointsPolicy      = "NO_POINTS_DEFAULT"
    resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
    severity            = "SEVERITY_UNSPECIFIED"

    type : {
      expression : {
        checkExpression : "",
        program : <<-EOT
          let active_brokers = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="managed-kafka", resource_type="cluster",
            resource_id="${data.yandex_mdb_kafka_cluster.backend_kafka[0].name}",
            name="kafka_is_alive",
            host="*"
          };

          alarm_if(any_of(map(active_brokers, line -> avg(line) < 1)));
        EOT
      }
    }

    channels      = local.solomon_channels
    languageStyle = local.solomon_language_style

    annotations = {
      host = local.backend_kafka_service_active_brokers
      # we use common juggler.yandex-team installation
      # so we must add environment prefix to raw event
      service        = "${var.env_name}_${local.backend_kafka_service}"
      active_brokers = "{{expression.active_brokers}}"
    }

    windowSecs = 300
    delaySecs  = 0
  })
}

resource "ytr_solomon_alert" "backend_kafka_empty_space" {
  count = var.enable_logs ? 1 : 0

  project     = var.solomon_project
  name        = "backend_kafka_empty_space"
  description = "Backend kafka has free space"
  alert_id    = "backend_kafka_empty_space"

  raw_json = jsonencode({
    name                = "backend_kafka_empty_space"
    description         = "Backend kafka has free space"
    noPointsPolicy      = "NO_POINTS_DEFAULT"
    resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
    severity            = "SEVERITY_UNSPECIFIED"

    type : {
      expression : {
        checkExpression : "",
        program : <<-EOT
          let empty_space = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="managed-kafka",
            resource_type="cluster",
            resource_id="${data.yandex_mdb_kafka_cluster.backend_kafka[0].name}",
            name="disk.free_bytes",
            host="*"
          };

          alarm_if(any_of(map(empty_space, line -> avg(line) < ${var.kafka_empty_space_limit})));
        EOT
      }
    }

    channels      = local.solomon_channels
    languageStyle = local.solomon_language_style

    annotations = {
      host = local.backend_kafka_empty_space
      # we use common juggler.yandex-team installation
      # so we must add environment prefix to raw event
      service = "${var.env_name}_${local.backend_kafka_service}"
    }

    windowSecs = 300
    delaySecs  = 0
  })
}

resource "ytr_solomon_alert" "logs_transfer_pushed" {
  count = var.enable_logs ? 1 : 0

  project     = var.solomon_project
  name        = "logs_transfer_pushed"
  description = "Logs transfer pushed rows"
  alert_id    = "logs_transfer_pushed"

  raw_json = jsonencode({
    name                = "logs_transfer_pushed"
    description         = "Logs transfer pushed rows"
    noPointsPolicy      = "NO_POINTS_DEFAULT"
    resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
    severity            = "SEVERITY_UNSPECIFIED"

    type : {
      expression : {
        checkExpression : "",
        program : <<-EOT
          let rows = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="data-transfer",
            resource_id="${module.logs[0].backend_app_logs_transfer_id}",
            name="sinker.pusher.data.row_events_pushed"
          };

          let data = group_by_time(60s, 'avg', rows);
          let diff_last = last(diff(data));
          alarm_if(diff_last <= 0.1);
        EOT
      }
    }

    channels      = local.solomon_channels
    languageStyle = local.solomon_language_style

    annotations = {
      host = local.app_logs_transfer_rows_pushed
      # we use common juggler.yandex-team installation
      # so we must add environment prefix to raw event
      service = "${var.env_name}_${local.app_logs_transfer_service}"
    }

    windowSecs = 300
    delaySecs  = 0
  })
}

resource "ytr_solomon_alert" "logs_transfer_parsed" {
  count = var.enable_logs ? 1 : 0

  project     = var.solomon_project
  name        = "logs_transfer_parsed"
  description = "Logs transfer parsed rows"
  alert_id    = "logs_transfer_parsed"

  raw_json = jsonencode({
    name                = "logs_transfer_parsed"
    description         = "Logs transfer parsed rows"
    noPointsPolicy      = "NO_POINTS_DEFAULT"
    resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
    severity            = "SEVERITY_UNSPECIFIED"

    type : {
      expression : {
        checkExpression : "",
        program : <<-EOT
          let rows = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="data-transfer",
            resource_id="${module.logs[0].backend_app_logs_transfer_id}",
            name="publisher.data.parsed_rows"
          };

          let data = group_by_time(60s, 'avg', rows);
          let diff_last = last(diff(data));
          alarm_if(diff_last <= 0.1);
        EOT
      }
    }

    channels      = local.solomon_channels
    languageStyle = local.solomon_language_style

    annotations = {
      host = local.app_logs_transfer_rows_parsed
      # we use common juggler.yandex-team installation
      # so we must add environment prefix to raw event
      service = "${var.env_name}_${local.app_logs_transfer_service}"
    }

    windowSecs = 300
    delaySecs  = 0
  })
}

resource "ytr_solomon_alert" "logs_transfer_unparsed" {
  count = var.enable_logs ? 1 : 0

  project     = var.solomon_project
  name        = "logs_transfer_unparsed"
  description = "Logs transfer unparsed rows"
  alert_id    = "logs_transfer_unparsed"

  raw_json = jsonencode({
    name                = "logs_transfer_unparsed"
    description         = "Logs transfer unparsed rows"
    noPointsPolicy      = "NO_POINTS_DEFAULT"
    resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
    severity            = "SEVERITY_UNSPECIFIED"

    type : {
      expression : {
        checkExpression : "",
        program : <<-EOT
          let rows = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="data-transfer",
            resource_id="${module.logs[0].backend_app_logs_transfer_id}",
            name="publisher.data.unparsed_rows"
          };

          let data = group_by_time(60s, 'avg', rows);
          let diff_last = last(diff(data));
          alarm_if(diff_last > 0);
        EOT
      }
    }

    channels      = local.solomon_channels
    languageStyle = local.solomon_language_style

    annotations = {
      host = local.app_logs_transfer_rows_unparsed
      # we use common juggler.yandex-team installation
      # so we must add environment prefix to raw event
      service = "${var.env_name}_${local.app_logs_transfer_service}"
    }

    windowSecs = 300
    delaySecs  = 0
  })
}

resource "ytr_solomon_alert" "logs_clickhouse_empty_space" {
  count = var.enable_logs ? 1 : 0

  project     = var.solomon_project
  name        = "logs_clickhouse_empty_space"
  description = "Logs clickhouse has free space"
  alert_id    = "logs_clickhouse_empty_space"

  raw_json = jsonencode({
    name                = "logs_clickhouse_empty_space"
    description         = "Logs clickhouse has free space"
    noPointsPolicy      = "NO_POINTS_DEFAULT"
    resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
    severity            = "SEVERITY_UNSPECIFIED"

    type : {
      expression : {
        checkExpression : "",
        program : <<-EOT
          let used = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="managed-clickhouse",
            resource_id="${module.logs[0].clickhouse_log_storage_id}",
            subcluster_name="clickhouse_subcluster",
            name="disk.used_bytes",
            host="*"
          };
          let total = {
            project="${local.solomon_project}",
            cluster="${local.solomon_cluster}",
            service="managed-clickhouse",
            resource_id="${module.logs[0].clickhouse_log_storage_id}",
            subcluster_name="clickhouse_subcluster",
            name="disk.total_bytes",
            host="*"
          };

          let ratio = used / total;

          warn_if(any_of(map(ratio, line -> last(line) > ${var.clickhouse_empty_space_warn_limit})));
          alarm_if(any_of(map(ratio, line -> last(line) > ${var.clickhouse_empty_space_alarm_limit})));
        EOT
      }
    }

    channels      = local.solomon_channels
    languageStyle = local.solomon_language_style

    annotations = {
      host = local.logs_clickhouse_empty_space
      # we use common juggler.yandex-team installation
      # so we must add environment prefix to raw event
      service = "${var.env_name}_${local.logs_clickhouse_service}"
    }

    windowSecs = 300
    delaySecs  = 0
  })
}

resource "ytr_solomon_alert" "container_cpu_usage" {
  for_each = local.k8s_service_names

  project  = var.solomon_project
  name     = "${each.key}_${local.container_cpu_usage}"
  alert_id = "${each.key}_${local.container_cpu_usage}"
  raw_json = jsonencode(
    {
      name          = "${each.key}_${local.container_cpu_usage}"
      description   = "Container CPU usage for ${each.key}"
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["namespace", "pod"]

      noPointsPolicy      = "NO_POINTS_NO_DATA"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_NO_DATA"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.container_cpu_usage_alert_window
      delaySecs           = local.container_cpu_usage_delay

      annotations = {
        service             = "${var.env_name}_${each.key}.{{labels.pod}}"
        host                = local.container_cpu_usage
        description         = "average container's CPU usage for last ${local.container_cpu_usage_alert_window / 60} minutes"
        cpu_usage           = "{{expression.percentage}}%"
        juggler_description = "CPU usage: {{expression.percentage}}%.${local.k8s_monitoring_description}"
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let usage = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-kubernetes",
              container="main",
              pod="${each.key}*",
              name="container.cpu.request_utilization"
            };
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.container_cpu_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "container_mem_usage" {
  for_each = local.k8s_service_names

  project  = var.solomon_project
  name     = "${each.key}_${local.container_mem_usage}"
  alert_id = "${each.key}_${local.container_mem_usage}"

  raw_json = jsonencode(
    {
      name          = "${each.key}_${local.container_mem_usage}"
      description   = "Container Memory usage for ${each.key}"
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["namespace", "pod"]

      noPointsPolicy      = "NO_POINTS_NO_DATA"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_NO_DATA"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.container_mem_usage_alert_window
      delaySecs           = local.container_mem_usage_delay

      annotations = {
        service             = "${var.env_name}_${each.key}.{{labels.pod}}"
        host                = local.container_mem_usage
        description         = "average container's Memory usage for last ${local.container_mem_usage_alert_window / 60} minutes"
        mem_usage           = "{{expression.percentage}}%"
        juggler_description = "MEM usage: {{expression.percentage}}%.${local.k8s_monitoring_description}"
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let usage = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-kubernetes",
              container="main",
              pod="${each.key}*",
              name="container.memory.request_utilization"
            };
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.container_mem_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "container_restarts" {
  for_each = local.k8s_service_names

  project  = var.solomon_project
  name     = "${each.key}_${local.container_restarts}"
  alert_id = "${each.key}_${local.container_restarts}"

  raw_json = jsonencode(
    {
      name          = "${each.key}_${local.container_restarts}"
      description   = "Container restarts for ${each.key}"
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["namespace", "pod"]

      noPointsPolicy      = "NO_POINTS_NO_DATA"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_NO_DATA"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.container_restarts_alert_window
      delaySecs           = local.container_restarts_delay

      annotations = {
        service             = "${var.env_name}_${each.key}.{{labels.pod}}"
        host                = local.container_restarts
        description         = "container's restart number for last ${local.container_restarts_alert_window / 60} minutes"
        restart_number      = "{{expression.value}}"
        juggler_description = "Restarts: {{expression.value}}.${local.k8s_monitoring_description}"
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let restarts_overall = series_max(
              {
                project="${local.solomon_project}",
                cluster="${local.solomon_cluster}",
                service="managed-kubernetes",
                container="main",
                pod="${each.key}*",
                name="container.restart_count"
              }
            );
            let restarts_new = diff(restarts_overall);
            let value = max(restarts_new);
            alarm_if(value > 0);
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "redis_is_alive" {
  project  = var.solomon_project
  name     = local.redis_is_alive
  alert_id = local.redis_is_alive

  raw_json = jsonencode(
    {
      name          = local.redis_is_alive
      description   = "Is redis cluster alive."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["subcluster_name", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.redis_is_alive_alert_window
      delaySecs           = local.redis_is_alive_delay

      annotations = {
        service             = "${var.env_name}.{{labels.subcluster_name}}.{{labels.host}}"
        host                = local.redis_is_alive
        description         = "is redis cluster alive for last ${local.redis_is_alive_alert_window / 60} minutes"
        is_alive            = "{{expression.value}}"
        juggler_description = "Cluster: {{labels.subcluster_name}}."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let is_alive = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-redis",
              name="redis_is_alive"
            };
            let value = min(series_min(is_alive));
            alarm_if(value < 1);
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "redis_cpu_usage" {
  project  = var.solomon_project
  name     = local.redis_cpu_usage
  alert_id = local.redis_cpu_usage

  raw_json = jsonencode(
    {
      name          = local.redis_cpu_usage
      description   = "redis cluster CPU usage."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["subcluster_name", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.redis_cpu_usage_alert_window
      delaySecs           = local.redis_cpu_usage_delay

      annotations = {
        service             = "${var.env_name}.{{labels.subcluster_name}}.{{labels.host}}"
        host                = local.redis_cpu_usage
        description         = "average redis clusters's CPU usage for last ${local.redis_cpu_usage_alert_window / 60} minutes"
        cpu_usage           = "{{expression.percentage}}%"
        juggler_description = "Cluster: {{labels.subcluster_name}}. CPU usage: {{expression.percentage}}%."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let cpu_idle = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-redis",
              name="cpu.idle"
            };
            let usage = 1 - series_avg(cpu_idle)/100;
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.redis_cpu_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "redis_mem_usage" {
  project  = var.solomon_project
  name     = local.redis_mem_usage
  alert_id = local.redis_mem_usage

  raw_json = jsonencode(
    {
      name          = local.redis_mem_usage
      description   = "redis cluster MEM usage."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["subcluster_name", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.redis_mem_usage_alert_window
      delaySecs           = local.redis_mem_usage_delay

      annotations = {
        service             = "${var.env_name}.{{labels.subcluster_name}}.{{labels.host}}"
        host                = local.redis_mem_usage
        description         = "average redis clusters's MEM usage for last ${local.redis_mem_usage_alert_window / 60} minutes"
        mem_usage           = "{{expression.percentage}}%"
        juggler_description = "Cluster: {{labels.subcluster_name}}. MEM usage: {{expression.percentage}}%."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let available_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-redis",
              name="mem.available_bytes"
            };
            let total_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-redis",
              name="mem.total_bytes"
            };
            let usage = 1 - series_avg(available_bytes/total_bytes);
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.redis_mem_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "redis_disk_usage" {
  project  = var.solomon_project
  name     = local.redis_disk_usage
  alert_id = local.redis_disk_usage

  raw_json = jsonencode(
    {
      name          = local.redis_disk_usage
      description   = "redis cluster DISK usage."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["subcluster_name", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.redis_disk_usage_alert_window
      delaySecs           = local.redis_disk_usage_delay

      annotations = {
        service             = "${var.env_name}.{{labels.subcluster_name}}.{{labels.host}}"
        host                = local.redis_disk_usage
        description         = "average redis clusters's DISK usage for last ${local.redis_disk_usage_alert_window / 60} minutes"
        disk_usage          = "{{expression.percentage}}%"
        juggler_description = "Cluster: {{labels.subcluster_name}}. DISK usage: {{expression.percentage}}%."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let free_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-redis",
              name="disk.free_bytes"
            };
            let total_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-redis",
              name="disk.total_bytes"
            };
            let usage = 1 - series_avg(free_bytes/total_bytes);
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.redis_disk_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "postgresql_is_alive" {
  project  = var.solomon_project
  name     = local.postgresql_is_alive
  alert_id = local.postgresql_is_alive

  raw_json = jsonencode(
    {
      name          = local.postgresql_is_alive
      description   = "Is postgresql cluster alive."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["subcluster_name", "resource_id", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.postgresql_is_alive_alert_window
      delaySecs           = local.postgresql_is_alive_delay

      annotations = {
        service             = "${var.env_name}.{{labels.subcluster_name}}.{{labels.host}}"
        host                = local.postgresql_is_alive
        description         = "is postgresql cluster alive for last ${local.postgresql_is_alive_alert_window / 60} minutes"
        is_alive            = "{{expression.value}}"
        juggler_description = "Cluster: {{labels.subcluster_name}}."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let is_alive = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-postgresql",
              name="postgres-is_alive"
            };
            let value = min(series_min(is_alive));
            alarm_if(value < 1);
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "postgresql_pooler_is_alive" {
  project  = var.solomon_project
  name     = local.postgresql_pooler_is_alive
  alert_id = local.postgresql_pooler_is_alive

  raw_json = jsonencode(
    {
      name          = local.postgresql_pooler_is_alive
      description   = "Is postgresql pooler alive."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["subcluster_name", "resource_id", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.postgresql_pooler_is_alive_alert_window
      delaySecs           = local.postgresql_pooler_is_alive_delay

      annotations = {
        service             = "${var.env_name}.{{labels.subcluster_name}}.{{labels.host}}"
        host                = local.postgresql_pooler_is_alive
        description         = "is postgresql pooler alive for last ${local.postgresql_pooler_is_alive_alert_window / 60} minutes"
        is_alive            = "{{expression.value}}"
        juggler_description = "Cluster: {{labels.subcluster_name}}."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let is_alive = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-postgresql",
              name="pooler-is_alive"
            };
            let value = min(series_min(is_alive));
            alarm_if(value < 1);
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "postgresql_cpu_usage" {
  project  = var.solomon_project
  name     = local.postgresql_cpu_usage
  alert_id = local.postgresql_cpu_usage

  raw_json = jsonencode(
    {
      name          = local.postgresql_cpu_usage
      description   = "Postgresql cluster CPU usage."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["subcluster_name", "resource_id", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.postgresql_cpu_usage_alert_window
      delaySecs           = local.postgresql_cpu_usage_delay

      annotations = {
        service             = "${var.env_name}.{{labels.subcluster_name}}.{{labels.host}}"
        host                = local.postgresql_cpu_usage
        description         = "average postgresql clusters's CPU usage for last ${local.postgresql_cpu_usage_alert_window / 60} minutes"
        cpu_usage           = "{{expression.percentage}}%"
        juggler_description = "Cluster: {{labels.subcluster_name}}. CPU usage: {{expression.percentage}}%."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let cpu_idle = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-postgresql",
              name="cpu.idle"
            };
            let usage = 1 - series_avg(cpu_idle)/100;
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.postgresql_cpu_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "postgresql_mem_usage" {
  project  = var.solomon_project
  name     = local.postgresql_mem_usage
  alert_id = local.postgresql_mem_usage

  raw_json = jsonencode(
    {
      name          = local.postgresql_mem_usage
      description   = "Postgresql cluster MEM usage."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["subcluster_name", "resource_id", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.postgresql_mem_usage_alert_window
      delaySecs           = local.postgresql_mem_usage_delay

      annotations = {
        service             = "${var.env_name}.{{labels.subcluster_name}}.{{labels.host}}"
        host                = local.postgresql_mem_usage
        description         = "average postgresql clusters's MEM usage for last ${local.postgresql_mem_usage_alert_window / 60} minutes"
        mem_usage           = "{{expression.percentage}}%"
        juggler_description = "Cluster: {{labels.subcluster_name}}. MEM usage: {{expression.percentage}}%."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let available_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-postgresql",
              name="mem.available_bytes"
            };
            let total_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-postgresql",
              name="mem.total_bytes"
            };
            let usage = 1 - series_avg(available_bytes/total_bytes);
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.postgresql_mem_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "postgresql_disk_usage" {
  project  = var.solomon_project
  name     = local.postgresql_disk_usage
  alert_id = local.postgresql_disk_usage

  raw_json = jsonencode(
    {
      name          = local.postgresql_disk_usage
      description   = "Postgresql cluster DISK usage."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["subcluster_name", "resource_id", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.postgresql_disk_usage_alert_window
      delaySecs           = local.postgresql_disk_usage_delay

      annotations = {
        service             = "${var.env_name}.{{labels.subcluster_name}}.{{labels.host}}"
        host                = local.postgresql_disk_usage
        description         = "average postgresql clusters's DISK usage for last ${local.postgresql_disk_usage_alert_window / 60} minutes"
        disk_usage          = "{{expression.percentage}}%"
        juggler_description = "Cluster: {{labels.subcluster_name}}. DISK usage: {{expression.percentage}}%."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let free_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-postgresql",
              name="disk.free_bytes"
            };
            let total_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-postgresql",
              name="disk.total_bytes"
            };
            let usage = 1 - series_avg(free_bytes/total_bytes);
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.postgresql_disk_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "postgresql_replication_lag" {
  project  = var.solomon_project
  name     = local.postgresql_replication_lag
  alert_id = local.postgresql_replication_lag

  raw_json = jsonencode(
    {
      name          = local.postgresql_replication_lag
      description   = "Postgresql cluster replication lag."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["subcluster_name", "resource_id", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.postgresql_replication_lag_alert_window
      delaySecs           = local.postgresql_replication_lag_delay

      annotations = {
        service             = "${var.env_name}.{{labels.subcluster_name}}.{{labels.host}}"
        host                = local.postgresql_replication_lag
        description         = "average postgresql clusters's replication lag for last ${local.postgresql_replication_lag_alert_window / 60} minutes"
        replication_lag     = "{{expression.value}}"
        juggler_description = "Cluster: {{labels.subcluster_name}}. Replication lag: {{expression.value}}."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let replication_lag = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-postgresql",
              name="postgres-replication_lag"
            };
            let value = avg(series_avg(replication_lag));
            alarm_if(value > ${local.postgresql_replication_lag_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "clickhouse_is_alive" {
  project  = var.solomon_project
  name     = local.clickhouse_is_alive
  alert_id = local.clickhouse_is_alive

  raw_json = jsonencode(
    {
      name          = local.clickhouse_is_alive
      description   = "Is clickhouse cluster alive."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["resource_id", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.clickhouse_is_alive_alert_window
      delaySecs           = local.clickhouse_is_alive_delay

      annotations = {
        service             = "${var.env_name}.{{labels.resource_id}}.{{labels.host}}"
        host                = local.clickhouse_is_alive
        description         = "is clickhouse cluster alive for last ${local.clickhouse_is_alive_alert_window / 60} minutes"
        is_alive            = "{{expression.value}}"
        juggler_description = "Cluster: {{labels.resource_id}}."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let is_alive = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-clickhouse",
              subcluster_name="clickhouse_subcluster",
              name="is_alive"
            };
            let value = min(series_min(is_alive));
            alarm_if(value < 1);
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "clickhouse_cpu_usage" {
  project  = var.solomon_project
  name     = local.clickhouse_cpu_usage
  alert_id = local.clickhouse_cpu_usage

  raw_json = jsonencode(
    {
      name          = local.clickhouse_cpu_usage
      description   = "clickhouse cluster CPU usage."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["resource_id", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.clickhouse_cpu_usage_alert_window
      delaySecs           = local.clickhouse_cpu_usage_delay

      annotations = {
        service             = "${var.env_name}.{{labels.resource_id}}.{{labels.host}}"
        host                = local.clickhouse_cpu_usage
        description         = "average clickhouse clusters's CPU usage for last ${local.clickhouse_cpu_usage_alert_window / 60} minutes"
        cpu_usage           = "{{expression.percentage}}%"
        juggler_description = "Cluster: {{labels.resource_id}}. CPU usage: {{expression.percentage}}%."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let cpu_idle = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-clickhouse",
              subcluster_name="clickhouse_subcluster",
              name="cpu.idle"
            };
            let usage = 1 - series_avg(cpu_idle)/100;
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.clickhouse_cpu_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "clickhouse_mem_usage" {
  project  = var.solomon_project
  name     = local.clickhouse_mem_usage
  alert_id = local.clickhouse_mem_usage

  raw_json = jsonencode(
    {
      name          = local.clickhouse_mem_usage
      description   = "clickhouse cluster MEM usage."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["resource_id", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.clickhouse_mem_usage_alert_window
      delaySecs           = local.clickhouse_mem_usage_delay

      annotations = {
        service             = "${var.env_name}.{{labels.resource_id}}.{{labels.host}}"
        host                = local.clickhouse_mem_usage
        description         = "average clickhouse clusters's MEM usage for last ${local.clickhouse_mem_usage_alert_window / 60} minutes"
        mem_usage           = "{{expression.percentage}}%"
        juggler_description = "Cluster: {{labels.resource_id}}. MEM usage: {{expression.percentage}}%."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let available_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-clickhouse",
              subcluster_name="clickhouse_subcluster",
              name="mem.available_bytes"
            };
            let total_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-clickhouse",
              subcluster_name="clickhouse_subcluster",
              name="mem.total_bytes"
            };
            let usage = 1 - series_avg(available_bytes/total_bytes);
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.clickhouse_mem_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "clickhouse_disk_usage" {
  project  = var.solomon_project
  name     = local.clickhouse_disk_usage
  alert_id = local.clickhouse_disk_usage

  raw_json = jsonencode(
    {
      name          = local.clickhouse_disk_usage
      description   = "clickhouse cluster DISK usage."
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["resource_id", "host"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.clickhouse_disk_usage_alert_window
      delaySecs           = local.clickhouse_disk_usage_delay

      annotations = {
        service             = "${var.env_name}.{{labels.resource_id}}.{{labels.host}}"
        host                = local.clickhouse_disk_usage
        description         = "average clickhouse clusters's DISK usage for last ${local.clickhouse_disk_usage_alert_window / 60} minutes"
        disk_usage          = "{{expression.percentage}}%"
        juggler_description = "Cluster: {{labels.resource_id}}. DISK usage: {{expression.percentage}}%."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let free_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-clickhouse",
              subcluster_name="clickhouse_subcluster",
              name="disk.free_bytes"
            };
            let total_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="managed-clickhouse",
              subcluster_name="clickhouse_subcluster",
              name="disk.total_bytes"
            };
            let usage = 1 - series_avg(free_bytes/total_bytes);
            let value = avg(usage);
            let percentage = round(value * 100);
            alarm_if(value > ${local.clickhouse_disk_usage_threshold});
            EOT
        }
      }
    }
  )
}

resource "ytr_solomon_alert" "file_storage_space_usage" {
  count = var.file_connector_alerts_enabled ? 1 : 0

  project  = var.solomon_project
  name     = local.file_storage_space_usage
  alert_id = local.file_storage_space_usage

  raw_json = jsonencode(
    {
      name          = local.file_storage_space_usage
      description   = "S3 buckets SPACE usage (file-connector)"
      channels      = local.solomon_channels
      languageStyle = local.solomon_language_style
      groupByLabels = ["resource_id"]

      noPointsPolicy      = "NO_POINTS_DEFAULT"
      resolvedEmptyPolicy = "RESOLVED_EMPTY_DEFAULT"
      severity            = "SEVERITY_UNSPECIFIED"
      windowSecs          = local.file_storage_space_usage_alert_window
      delaySecs           = local.file_storage_space_usage_delay

      annotations = {
        service             = "${var.env_name}.{{labels.resource_id}}"
        host                = local.file_storage_space_usage
        description         = "average file-connector S3 buckets' SPACE usage over the last ${local.file_storage_space_usage_alert_window / 60} minutes"
        space_usage         = "{{expression.usage_percentage}}%"
        juggler_description = "Bucket: {{labels.resource_id}}. SPACE usage: {{expression.usage_percentage}}%."
      }

      type = {
        expression = {
          checkExpression = ""
          program         = <<-EOT
            let used_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="storage",
              resource_type="bucket",
              resource_id="${join("|", var.file_connector_bucket_names)}",
              storage_type="All",
              name="space_usage"
            };
            let max_bytes = {
              project="${local.solomon_project}",
              cluster="${local.solomon_cluster}",
              service="storage",
              resource_type="bucket",
              resource_id="${join("|", var.file_connector_bucket_names)}",
              name="max_size"
            };
            let usage = avg(series_avg(used_bytes / max_bytes));
            let usage_percentage = round(usage * 100);
            alarm_if(usage > ${local.file_storage_space_usage_threshold});
            EOT
        }
      }
    }
  )
}
