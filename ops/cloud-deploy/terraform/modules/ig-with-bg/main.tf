locals {

}


data "ycp_microcosm_instance_group_instance_group" "ig" {
  instance_group_id = var.instance_group_id
}

resource "ycp_platform_alb_backend_group" "bg" {
  name = var.name
  folder_id = var.folder_id
  description = var.description
  http {
    backend {
      name = "http-backend"
      weight = 100
      port = var.use_tls ? 443 : 80
      target_group {
        target_group_id = data.ycp_microcosm_instance_group_instance_group.ig.platform_l7_load_balancer_state.0.target_group_id
      }
      dynamic "tls" {
        for_each = var.use_tls && var.tls_class == "high" ? [1]:[]
        content {
          tls_options {
            tls_max_version = "TLS_V1_2"
            tls_min_version = "TLS_V1_2"
          }
        }
      }
      // Just for example how to make switch/case for nested blocks
      dynamic "tls" {
        for_each = var.use_tls && var.tls_class == "low" ? [1]:[]
        content {
          tls_options {
            tls_max_version = "TLS_V1_0"
            tls_min_version = "TLS_V1_0"
          }
        }
      }
    }
    connection {}
  }
}
