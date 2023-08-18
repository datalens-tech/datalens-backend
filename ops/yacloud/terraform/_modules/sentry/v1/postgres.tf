resource "yandex_mdb_postgresql_cluster" "sentry" {
  name        = "sentry"
  environment = "PRODUCTION"
  network_id  = var.network_id

  config {
    version = 14
    resources {
      resource_preset_id = var.postgresql_config.preset
      disk_type_id       = "network-ssd"
      disk_size          = 16
    }
  }
  dynamic "host" {
    for_each = var.postgresql_config.locations
    content {
      subnet_id = host.value["subnet_id"]
      zone      = host.value["zone"]
    }
  }
}

resource "yandex_mdb_postgresql_user" "sentry_user" {
  cluster_id = yandex_mdb_postgresql_cluster.sentry.id
  name       = "sentry_user"
  password   = random_password.sentry_pg_password.result
  conn_limit = 50
  settings = {
    default_transaction_isolation = "read committed"
    log_min_duration_statement    = 5000
  }
}

resource "yandex_mdb_postgresql_database" "sentry_db" {
  cluster_id = yandex_mdb_postgresql_cluster.sentry.id
  name       = "sentry_db"
  owner      = yandex_mdb_postgresql_user.sentry_user.name

  extension {
    name = "citext"
  }
}
