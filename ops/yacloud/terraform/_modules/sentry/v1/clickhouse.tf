resource "yandex_mdb_clickhouse_cluster" "clickhouse" {
  count = 0 # sentry doesn't work with fresh ch. now it uses 19.14 in k8s

  name        = "sentry_clickhouse"
  environment = "PRODUCTION"
  network_id  = var.network_id

  clickhouse {
    resources {
      resource_preset_id = "s2.micro"
      disk_type_id       = "network-ssd"
      disk_size          = 10
    }
  }

  database {
    name = "sentry"
  }

  user {
    name     = "sentry_user"
    password = "password"
    permission {
      database_name = "sentry"
    }
  }
  host {
    type      = "CLICKHOUSE"
    zone      = "ru-central1-a"
    subnet_id = "buct4cp7lc8p0p2rht5l"
  }
}
