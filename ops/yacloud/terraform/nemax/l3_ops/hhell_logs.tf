resource "yandex_mdb_clickhouse_cluster" "log-storage" {
  environment        = "PRODUCTION"
  name               = "hhell_logs"
  network_id         = "btceuge29d4pevsjsji7"
  version            = "23.3"
  embedded_keeper    = true
  security_group_ids = [yandex_vpc_security_group.for_hhell_logs.id]

  clickhouse {
    resources {
      disk_size          = 20
      disk_type_id       = "network-ssd"
      resource_preset_id = "s3-c2-m8"
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

  host {
    type       = "CLICKHOUSE"
    shard_name = "shard1"
    zone       = "eu-north1-a"
    subnet_id  = "dc7uf6ffesobi7md8rd5"
  }

  database {
    name = "logs"
  }

  user {
    name     = "log_writer"
    password = "CHANGEME"
    permission {
      database_name = "logs"
    }
    settings {
      async_insert              = true
      async_insert_busy_timeout = 10000
    }
  }
}

resource "yandex_vpc_security_group" "for_hhell_logs" {
  network_id = "btceuge29d4pevsjsji7"

  ingress {
    protocol       = "ANY"
    description    = "ANY in"
    v4_cidr_blocks = ["0.0.0.0/0"]
    v6_cidr_blocks = ["::/0"]
    from_port      = 0
    to_port        = 65535
  }
  egress {
    protocol       = "ANY"
    description    = "ANY out"
    v4_cidr_blocks = ["0.0.0.0/0"]
    v6_cidr_blocks = ["::/0"]
    from_port      = 0
    to_port        = 65535
  }

  ingress {
    protocol    = "ANY"
    description = "ANY from main net"
    v6_cidr_blocks = [
      "2a13:5947:10:0:9010:45::/112",
      "2a13:5947:11:0:9010:45::/112",
      "2a13:5947:12:0:9010:45::/112",
    ]
  }
}
