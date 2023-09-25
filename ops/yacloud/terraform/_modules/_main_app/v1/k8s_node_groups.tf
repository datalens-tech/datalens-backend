data "yandex_vpc_security_group" "main" { # TODO: from infra_data
  name = "k8s-main-sg"
}

locals {
  map_node_group_key_node_group_discriminator = {
    for ng_code in keys(var.k8s_node_groups) :
    ng_code => "${var.env_name}-${ng_code}"
  }
  ng_discriminator_node_label = "datalens.cloud.yandex.net/ng_discriminator"
}

resource "yandex_kubernetes_node_group" "this" {
  for_each = var.k8s_node_groups

  cluster_id = module.infra_data.k8s_cluster.id

  name = format("%s-%s", "k8s", replace(each.key, "_", "-"))
  instance_template {
    platform_id = "standard-v3"
    network_interface {
      ipv4       = true
      ipv6       = true
      nat        = false
      subnet_ids = module.subinfra_data.locations[*].subnet_id
      security_group_ids = [
        data.yandex_vpc_security_group.main.id,
      ]
    }

    resources {
      memory        = each.value.memory
      cores         = each.value.cores
      core_fraction = each.value.core_fraction
    }

    boot_disk {
      type = "network-hdd"
      size = 30
    }

    scheduling_policy {
      preemptible = false
    }

  }

  scale_policy {
    fixed_scale {
      size = each.value.size
    }
  }

  allocation_policy {
    dynamic "location" {
      for_each = module.subinfra_data.locations
      content {
        zone = location.value["zone"]
      }
    }
  }

  maintenance_policy {
    auto_upgrade = true
    auto_repair  = true
  }

  node_labels = {
    (local.ng_discriminator_node_label) = local.map_node_group_key_node_group_discriminator[each.key]
  }

  node_taints = each.value.dedicated ? ["dedicated=${local.map_node_group_key_node_group_discriminator[each.key]}:NoSchedule"] : []

}
