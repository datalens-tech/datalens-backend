locals {
  yc_alb_internal_ca_secret_name = "internal-ca"
}

resource "kubernetes_secret" "yc_alb_internal_ca" {
  metadata {
    name      = local.yc_alb_internal_ca_secret_name
    namespace = var.k8s_namespace
  }
  data = { "internal-root-ca.pem" = var.internal_cert }
}
