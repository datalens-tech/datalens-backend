locals {
  chart_name         = "yc-alb-ingress-controller"
  controller_version = "v0.1.3"

  sa_key = {
    id                 = yandex_iam_service_account_key.this.id
    created_at         = yandex_iam_service_account_key.this.created_at
    key_algorithm      = yandex_iam_service_account_key.this.key_algorithm
    public_key         = yandex_iam_service_account_key.this.public_key
    private_key        = yandex_iam_service_account_key.this.private_key
    service_account_id = yandex_iam_service_account_key.this.service_account_id
  }
}

resource "kubernetes_namespace" "this" {
  metadata {
    name = var.k8s_namespace
  }
}

resource "helm_release" "this" {
  name       = local.chart_name
  repository = "oci://cr.yandex/yc"
  chart      = "yc-alb-ingress-controller-chart"
  version    = local.controller_version

  timeout   = 240
  namespace = var.k8s_namespace

  values = [
    yamlencode({
      folderId                 = var.folder_id
      clusterId                = var.cluster_id
      saKeySecretKey           = jsonencode(local.sa_key)
      endpoint                 = var.cloud_api_endpoint
      internalRootCaSecretName = var.use_internal_ca ? local.yc_alb_internal_ca_secret_name : null
    })
  ]

  depends_on = [kubernetes_namespace.this]
}
