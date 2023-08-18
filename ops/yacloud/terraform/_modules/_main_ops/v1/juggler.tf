resource "ytr_juggler_aggregate" "back_alb_5xx" {
  project = var.juggler_project
  host    = local.alb_5xx
  service = local.alb_5xx_service
  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_crit"
    }
    children = [
      for app_info in var.app_info : {
        service = "${var.env_name}_${local.alb_5xx_service}"
        host    = app_info.name
        type    = "HOST"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "back_alb_4xx_velocity" {
  count = var.alb_4xx_velocity_alert.enabled ? 1 : 0

  project = var.juggler_project
  host    = local.alb_4xx_velocity
  service = local.alb_4xx_velocity_service
  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_crit"
    }
    children = [
      for app_info in var.app_info : {
        service = "${var.env_name}_${local.alb_4xx_velocity_service}"
        host    = app_info.name
        type    = "HOST"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "backend_kafka_active_brokers" {
  count = var.enable_logs ? 1 : 0

  project = var.juggler_project
  host    = local.backend_kafka_service_active_brokers
  service = local.backend_kafka_service
  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_crit"
    }
    children = [
      {
        service = "${var.env_name}_${local.backend_kafka_service}"
        host    = local.backend_kafka_service_active_brokers
        type    = "HOST"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "backend_kafka_empty_space" {
  count = var.enable_logs ? 1 : 0

  project = var.juggler_project
  host    = local.backend_kafka_empty_space
  service = local.backend_kafka_service
  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_crit"
    }
    children = [
      {
        service = "${var.env_name}_${local.backend_kafka_service}"
        host    = local.backend_kafka_empty_space
        type    = "HOST"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "app_logs_transfer_pushed" {
  count = var.enable_logs ? 1 : 0

  project = var.juggler_project
  host    = local.app_logs_transfer_rows_pushed
  service = local.app_logs_transfer_service
  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_crit"
    }
    children = [
      {
        service = "${var.env_name}_${local.app_logs_transfer_service}"
        host    = local.app_logs_transfer_rows_pushed
        type    = "HOST"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "app_logs_transfer_parsed" {
  count = var.enable_logs ? 1 : 0

  project = var.juggler_project
  host    = local.app_logs_transfer_rows_parsed
  service = local.app_logs_transfer_service
  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_crit"
    }
    children = [
      {
        service = "${var.env_name}_${local.app_logs_transfer_service}"
        host    = local.app_logs_transfer_rows_parsed
        type    = "HOST"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "app_logs_transfer_unparsed" {
  count = var.enable_logs ? 1 : 0

  project = var.juggler_project
  host    = local.app_logs_transfer_rows_unparsed
  service = local.app_logs_transfer_service
  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_crit"
    }
    children = [
      {
        service = "${var.env_name}_${local.app_logs_transfer_service}"
        host    = local.app_logs_transfer_rows_unparsed
        type    = "HOST"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "logs_clickhouse_empty_space" {
  count = var.enable_logs ? 1 : 0

  project = var.juggler_project
  host    = local.logs_clickhouse_empty_space
  service = local.logs_clickhouse_service
  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_crit"
    }
    children = [
      {
        service = "${var.env_name}_${local.logs_clickhouse_service}"
        host    = local.logs_clickhouse_empty_space
        type    = "HOST"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "container_cpu_usage" {
  for_each = local.k8s_service_names

  project = var.juggler_project
  service = each.key
  host    = local.container_cpu_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_ok"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.container_cpu_usage})&(service=${var.env_name}_${each.key}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "container_mem_usage" {
  for_each = local.k8s_service_names

  project = var.juggler_project
  service = each.key
  host    = local.container_mem_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_ok"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.container_mem_usage})&(service=${var.env_name}_${each.key}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "container_restarts" {
  for_each = local.k8s_service_names

  project = var.juggler_project
  service = each.key
  host    = local.container_restarts

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "force_ok"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.container_restarts})&(service=${var.env_name}_${each.key}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "postgresql_is_alive" {
  project = var.juggler_project
  service = local.postgresql_service
  host    = local.postgresql_is_alive

  raw_yaml = yamlencode({
    refresh_time = 10
    ttl          = 600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.postgresql_is_alive})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "postgresql_pooler_is_alive" {
  project = var.juggler_project
  service = local.postgresql_service
  host    = local.postgresql_pooler_is_alive

  raw_yaml = yamlencode({
    refresh_time = 10
    ttl          = 600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.postgresql_pooler_is_alive})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "postgresql_cpu_usage" {
  project = var.juggler_project
  service = local.postgresql_service
  host    = local.postgresql_cpu_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.postgresql_cpu_usage})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "postgresql_mem_usage" {
  project = var.juggler_project
  service = local.postgresql_service
  host    = local.postgresql_mem_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.postgresql_mem_usage})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "postgresql_disk_usage" {
  project = var.juggler_project
  service = local.postgresql_service
  host    = local.postgresql_disk_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.postgresql_disk_usage})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "postgresql_replication_lag" {
  project = var.juggler_project
  service = local.postgresql_service
  host    = local.postgresql_replication_lag

  raw_yaml = yamlencode({
    refresh_time = 10
    ttl          = 600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.postgresql_replication_lag})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "redis_is_alive" {
  project = var.juggler_project
  service = local.redis_service
  host    = local.redis_is_alive

  raw_yaml = yamlencode({
    refresh_time = 10
    ttl          = 600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.redis_is_alive})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}
resource "ytr_juggler_aggregate" "redis_cpu_usage" {
  project = var.juggler_project
  service = local.redis_service
  host    = local.redis_cpu_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.redis_cpu_usage})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "redis_mem_usage" {
  project = var.juggler_project
  service = local.redis_service
  host    = local.redis_mem_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.redis_mem_usage})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "redis_disk_usage" {
  project = var.juggler_project
  service = local.redis_service
  host    = local.redis_disk_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.redis_disk_usage})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "clickhouse_is_alive" {
  project = var.juggler_project
  service = local.clickhouse_service
  host    = local.clickhouse_is_alive

  raw_yaml = yamlencode({
    refresh_time = 10
    ttl          = 600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.clickhouse_is_alive})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}
resource "ytr_juggler_aggregate" "clickhouse_cpu_usage" {
  project = var.juggler_project
  service = local.clickhouse_service
  host    = local.clickhouse_cpu_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.clickhouse_cpu_usage})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "clickhouse_mem_usage" {
  project = var.juggler_project
  service = local.clickhouse_service
  host    = local.clickhouse_mem_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.clickhouse_mem_usage})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "clickhouse_disk_usage" {
  project = var.juggler_project
  service = local.clickhouse_service
  host    = local.clickhouse_disk_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.clickhouse_disk_usage})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_aggregate" "file_storage_space_usage" {
  count = var.file_connector_alerts_enabled ? 1 : 0

  project = var.juggler_project
  service = local.storage_service
  host    = local.file_storage_space_usage

  raw_yaml = yamlencode({
    refresh_time = 90
    ttl          = 3600
    aggregator   = "logic_or"
    aggregator_kwargs = {
      nodata_mode = "skip"
    }
    children = [
      {
        type     = "EVENTS"
        host     = "(host=${local.file_storage_space_usage})&(service=${var.env_name}.*)"
        service  = "all"
        instance = "all"
      }
    ]
  })
}

resource "ytr_juggler_notify_rule" "back_telegram_notification" {
  project     = var.juggler_project
  description = "backend notifications to telegram chat"
  raw_yaml = yamlencode({
    selector = "project=${var.juggler_project}"
    template_kwargs = {
      login = [
        var.telegram_chat
      ]
      method = [
        "telegram"
      ]
      status = [
        "WARN",
        "CRIT",
        "OK"
      ]
    }
    template_name = "on_status_change"
  })
}

resource "ytr_juggler_notify_rule" "back_telegram_notification_repeat" {
  project     = var.juggler_project
  description = "repeat backend CRIT notifications to telegram chat"
  raw_yaml = yamlencode({
    selector = "project=${var.juggler_project}"
    template_kwargs = {
      login = [
        var.telegram_chat
      ]
      method = [
        "telegram"
      ]
      status = [
        "CRIT",
      ]
      repeat = 1800
      delay  = 1800
    }
    template_name = "on_status_change"
  })
}
