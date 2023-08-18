resource "kubernetes_config_map" "jaeger_data" {
  metadata {
    name      = "jaeger-data"
    namespace = var.apps_namespace
  }
  data = {
    "YandexInternalRootCA.crt" = var.internal_cert
  }
}
