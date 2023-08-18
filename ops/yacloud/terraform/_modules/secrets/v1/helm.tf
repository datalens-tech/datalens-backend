resource "helm_release" "this" {
  name       = "external-secrets"
  repository = "https://charts.external-secrets.io"
  chart      = "external-secrets"

  set {
    name  = "image.tag"
    value = "v0.5.8"
  }

  timeout   = 240
  namespace = var.k8s_eso_namespace

  depends_on = [kubernetes_namespace.eso]
}

resource "kubernetes_namespace" "eso" {
  metadata {
    name = var.k8s_eso_namespace
  }
}
