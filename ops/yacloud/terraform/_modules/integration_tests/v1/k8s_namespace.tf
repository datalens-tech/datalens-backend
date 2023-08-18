resource "kubernetes_namespace" "integration_tests" {
  metadata {
    name = var.k8s_namespace
  }
}
