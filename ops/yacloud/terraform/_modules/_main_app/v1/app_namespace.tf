resource "kubernetes_namespace" "app" {
  metadata {
    name = var.k8s_namespace
  }
}

moved {
  from = kubernetes_secret.internal_ca_cert
  to   = module.secret_store.kubernetes_secret.internal_ca_cert
}
