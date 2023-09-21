resource "yandex_mdb_redis_cluster" "this" {
  name               = var.name
  environment        = "PRODUCTION"
  network_id         = var.network_id
  security_group_ids = var.security_group_ids

  labels = var.disable_autopurge ? { mdb-auto-purge = "off" } : {}

  config {
    password = var.password
    version  = var.redis_version
  }

  resources {
    resource_preset_id = var.preset
    disk_size          = var.disk_size
  }

  dynamic "host" {
    for_each = var.locations
    content {
      subnet_id = host.value["subnet_id"]
      zone      = host.value["zone"]
    }
  }

  maintenance_window {
    type = "ANYTIME"
  }

  lifecycle {
    ignore_changes = [host[0].replica_priority]
  }
}
