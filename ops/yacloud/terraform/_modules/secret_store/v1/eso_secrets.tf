resource "kubernetes_secret" "eso_sa_key" {
  metadata {
    name      = "eso-yc-auth"
    namespace = var.k8s_namespace
  }
  data = var.eso_sa_key
}

resource "kubernetes_secret" "internal_ca_cert" {
  metadata {
    name      = "yc-internal-ca-cert"
    namespace = var.k8s_namespace
  }
  data = { "cert" = module.infra_data.internal_cert }
}
