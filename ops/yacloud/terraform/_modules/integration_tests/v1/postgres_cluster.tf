data "ycp_vpc_subnet" "dl_back_network_subnets" {
  for_each  = toset(var.subnet_ids)
  subnet_id = each.key
}

locals {
  locations = [for sn in data.ycp_vpc_subnet.dl_back_network_subnets : { subnet_id : sn.id, zone : sn.zone_id }]
}

resource "yandex_mdb_postgresql_cluster" "integration_tests" {
  name        = "integration-tests-pg"
  environment = "PRODUCTION"
  network_id  = yandex_vpc_network.this.id
  folder_id   = var.db_clusters_folder_id

  labels = { mdb-auto-purge = "off" }

  config {
    version = 14
    resources {
      resource_preset_id = var.pg_cluster_resource_preset_id
      disk_type_id       = "network-ssd"
      disk_size          = 10
    }
    access {
      data_lens = true
      web_sql   = true
    }
  }

  host {
    zone             = yandex_vpc_subnet.this.zone
    subnet_id        = yandex_vpc_subnet.this.id
    assign_public_ip = true
  }
}

resource "random_password" "pg_user_password" {
  length  = 32
  special = false
}

resource "yandex_mdb_postgresql_user" "integration_tests" {
  cluster_id = yandex_mdb_postgresql_cluster.integration_tests.id
  name       = "user1"
  password   = random_password.pg_user_password.result
}

resource "yandex_mdb_postgresql_database" "integration_tests" {
  cluster_id = yandex_mdb_postgresql_cluster.integration_tests.id
  name       = "db1"
  owner      = yandex_mdb_postgresql_user.integration_tests.name
}

locals {
  pg_host_fqdn = yandex_mdb_postgresql_cluster.integration_tests.host[0].fqdn
}

data "external" "pg_host_address" {
  program = ["sh", "${path.module}/scripts/host_address_tf_ext_resource.sh"]

  query = {
    # arbitrary map from strings to strings, passed
    # to the external program as the data query.
    fqdn = local.pg_host_fqdn
  }
}