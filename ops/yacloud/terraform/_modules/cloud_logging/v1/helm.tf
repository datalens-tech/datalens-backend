resource "kubernetes_namespace" "this" {
  metadata {
    name = var.k8s_namespace
  }
}

resource "helm_release" "this" {
  name  = "fluentbit-cl"
  chart = "${path.module}/fluentbit"
  # waiting for pr to be merged and deployed:
  # https://bb.yandexcloud.net/projects/CLOUD/repos/mk8s-marketplace-helm/pull-requests/151/overview
  #  repository = "oci://cr.yandex/yc-marketplace/yandex-cloud/fluent-bit"
  #  chart      = "fluent-bit-cloud-logging"
  #  version    = "1.0-7"

  timeout   = 240
  namespace = kubernetes_namespace.this.metadata[0].name

  values = [
    yamlencode({
      image            = "cr.yandex/yc-marketplace/yandex-cloud/fluent-bit/fluent-bit-plugin-yandex:v2.0.3-fluent-bit-1.9.3"
      loggingGroupId   = yandex_logging_group.this.id
      loggingFilter    = var.k8s_cluster_id
      cloudApiEndpoint = var.cloud_api_endpoint
      auth             = { json = jsonencode(local.sa_key) }
    })
  ]

  set {
    name = "trigger"
    value = filemd5("${path.module}/fluentbit/templates/config-map.yaml")
  }
}
