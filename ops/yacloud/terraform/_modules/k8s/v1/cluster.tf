resource "ycp_vpc_address" "master_v6" {
  count = var.use_ext_v6 ? 1 : 0

  name     = "k8s-master-v6-address"
  reserved = true

  ipv6_address_spec {
    requirements {
      hints = ["yandex-only"] # Address will be available only from Yandex' network
    }
  }
}

resource "yandex_kubernetes_cluster" "this" {
  release_channel         = "RAPID"
  name                    = var.cluster_name
  description             = "DataLens k8s cluster"
  network_id              = var.network_id
  node_service_account_id = yandex_iam_service_account.node.id
  service_account_id      = yandex_iam_service_account.cluster.id
  cluster_ipv4_range      = var.cluster_ipv4_range
  cluster_ipv6_range      = var.cluster_ipv6_range
  service_ipv4_range      = var.service_ipv4_range
  service_ipv6_range      = var.service_ipv6_range
  master {
    version             = var.k8s_version
    public_ip           = !var.use_ext_v6
    external_v6_address = var.use_ext_v6 ? ycp_vpc_address.master_v6[0].ipv6_address[0].address : null
    security_group_ids = var.bastion.enable ? [
      yandex_vpc_security_group.k8s_main.id,
      yandex_vpc_security_group.k8s_master_whitelist.id,
      yandex_vpc_security_group.k8s_bastion[0].id
      ] : [
      yandex_vpc_security_group.k8s_main.id,
      yandex_vpc_security_group.k8s_master_whitelist.id
    ]

    dynamic "regional" {
      for_each = length(var.locations) >= 3 ? [1] : []
      content {
        region = var.cluster_region
        dynamic "location" {
          for_each = var.locations
          content {
            subnet_id = location.value["subnet_id"]
            zone      = location.value["zone"]
          }
        }
      }
    }

    dynamic "zonal" {
      for_each = length(var.locations) >= 3 ? [] : [1]
      content {
        subnet_id = var.locations[0]["subnet_id"]
        zone      = var.locations[0]["zone"]
      }
    }

  }

  dynamic "network_implementation" {
    for_each = var.use_cilium ? [1] : []
    content {
      cilium {}
    }
  }

  labels = var.enable_silo ? {
    secplatform-deploy-group    = "canary"
    secplatform-force-bootstrap = true
  } : {}
}

locals {
  preferrable_address = (
    var.use_ext_v6 ?
    "[${yandex_kubernetes_cluster.this.master[0].external_v6_address}]" : yandex_kubernetes_cluster.this.master[0].external_v4_address
  )
  bastion_address = var.bastion.enable ? "https://${yandex_kubernetes_cluster.this.id}.${var.bastion.endpoint_suffix}" : null
  endpoint        = var.bastion.enable ? "${local.bastion_address}" : "https://${local.preferrable_address}"
  ca              = yandex_kubernetes_cluster.this.master[0].cluster_ca_certificate
}
