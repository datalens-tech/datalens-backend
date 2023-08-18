resource "yandex_mdb_clickhouse_cluster" "samples" {
  environment     = "PRODUCTION"
  name            = "samples"
  network_id      = yandex_vpc_network.samples.id
  version         = "22.8"
  embedded_keeper = true

  clickhouse {
    resources {
      disk_size          = 15
      disk_type_id       = "network-ssd"
      resource_preset_id = "s3-c4-m16"
    }

    config {
      timezone = "UTC"
    }
  }
  access {
    data_lens = true
    web_sql   = true
  }

  host {
    type      = "CLICKHOUSE"
    zone      = yandex_vpc_subnet.samples_il1-a.zone
    subnet_id = yandex_vpc_subnet.samples_il1-a.id
  }

  host {
    type      = "CLICKHOUSE"
    zone      = yandex_vpc_subnet.samples_il1-b.zone
    subnet_id = yandex_vpc_subnet.samples_il1-b.id
  }

  database {
    name = "samples"
  }

  user {
    name     = "samples_user_ro"
    password = "fakepassword"
    permission {
      database_name = "samples"
    }
    settings {
      readonly = 2
    }
  }
  user {
    name     = "samples_user_rw"
    password = "fakepassword"
    permission {
      database_name = "samples"
    }
  }
}
