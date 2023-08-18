locals {
  databases = var.enable_usage_tracking ? [
    local.clickhouse_backend_logs_database, local.clickhouse_usage_tracking_database
    ] : [
    local.clickhouse_backend_logs_database
  ]
}

resource "yandex_mdb_clickhouse_cluster" "log-storage" {
  environment     = "PRODUCTION"
  name            = var.clickhouse_settings.name
  network_id      = var.clickhouse_settings.network_id
  version         = local.clickhouse_version
  embedded_keeper = true

  labels = var.kafka_cluster.labels

  clickhouse {
    resources {
      disk_size          = var.clickhouse_settings.disk_size
      disk_type_id       = "network-ssd"
      resource_preset_id = var.clickhouse_settings.resource_preset_id
    }

    config {
      timezone = "UTC"
    }
  }

  #  cloud_storage {
  #    enabled = var.clickhouse_settings.cloud_storage
  #  }

  access {
    data_lens     = true
    web_sql       = true
    data_transfer = true
  }

  dynamic "host" {
    for_each = var.clickhouse_settings.ch_hosts
    content {
      type       = "CLICKHOUSE"
      shard_name = "shard1"
      zone       = host.value["zone"]
      subnet_id  = host.value["subnet_id"]
    }
  }

  dynamic "database" {
    for_each = local.databases
    content {
      name = database.value
    }
  }

  user {
    name     = local.clickhouse_transfer_user
    password = local.clickhouse_transfer_password
    dynamic "permission" {
      for_each = local.databases
      content {
        database_name = permission.value
      }
    }
  }

  user {
    name     = local.clickhouse_log_devops_user
    password = local.clickhouse_log_devops_password
    dynamic "permission" {
      for_each = local.databases
      content {
        database_name = permission.value
      }
    }
  }

  dynamic "user" {
    for_each = var.enable_usage_tracking ? [local.clickhouse_ut_ro_user] : []
    content {
      name     = user.value
      password = local.clickhouse_passwords[user.value]
      permission {
        database_name = local.clickhouse_usage_tracking_database
      }
      settings {
        readonly = 2
      }
    }
  }
}

resource "null_resource" "log-storage-app-logs-table" {
  provisioner "local-exec" {
    command = "${path.module}/create_ch_table.sh"
    environment = {
      CH_USER      = local.clickhouse_transfer_user
      CH_PASSWORD  = local.clickhouse_transfer_password
      CH_HOST      = yandex_mdb_clickhouse_cluster.log-storage.host[0].fqdn
      CH_CLUSTER   = yandex_mdb_clickhouse_cluster.log-storage.id
      CH_DATABASE  = local.clickhouse_backend_logs_database
      CH_TABLE     = local.clickhouse_backend_app_logs_table
      CA_CERT_PATH = var.internal_cert_path
    }
  }
}
