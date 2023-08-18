resource "yandex_mdb_clickhouse_cluster" "this" {
  count = var.ch_config.num

  environment = "PRODUCTION"
  name        = "file_connector_${count.index}"
  network_id  = var.network_id
  version     = var.ch_config.version

  description = format("FileConn CH in %s", var.ch_config.locations[count.index % length(var.ch_config.locations)]["zone"])

  labels = var.disable_autopurge ? { mdb-auto-purge = "off" } : {}

  clickhouse {
    resources {
      disk_size          = 10
      disk_type_id       = "network-hdd"
      resource_preset_id = var.ch_config.preset
    }
    config {
      timezone = "UTC"
    }
  }

  host {
    type      = "CLICKHOUSE"
    zone      = var.ch_config.locations[count.index % length(var.ch_config.locations)]["zone"]
    subnet_id = var.ch_config.locations[count.index % length(var.ch_config.locations)]["subnet_id"]
  }

  database {
    name = "dummy"
  }

  user {
    name     = "dl_file_conn"
    password = random_password.ch_user_password.result
    settings {
      readonly = 2
    }
  }
  maintenance_window {
    type = "WEEKLY"
    hour = 11 + count.index
    day  = "WED"
  }
}

resource "yandex_lockbox_secret" "ch_user" {
  folder_id = var.folder_id
  name      = "file_conn_ch_user"
}

resource "random_password" "ch_user_password" {
  length  = 32
  special = false
}

resource "yandex_lockbox_secret_version" "ch_user_sec_version" {
  secret_id = yandex_lockbox_secret.ch_user.id
  entries {
    key        = "password"
    text_value = random_password.ch_user_password.result
  }
}
