resource "kubernetes_secret" "cert_internal" {
  metadata {
    name      = "cert-internal"
    namespace = var.k8s_namespace
  }
  data = { "data" = module.infra_data.internal_cert }
}

resource "kubernetes_secret" "cert_bundle" {
  metadata {
    name      = "cert-bundle"
    namespace = var.k8s_namespace
  }
  data = { "data" = module.infra_data.cert_bundle }
}