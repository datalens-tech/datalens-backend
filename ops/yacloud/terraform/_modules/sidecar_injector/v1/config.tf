resource "kubernetes_config_map" "sidecars" {
  metadata {
    name      = "sidecars"
    namespace = local.namespace
    labels = {
      k8s-app = "sidecar-injector"
      track   = "prod"
    }
  }

  data = {
    "jaeger" = templatefile("${path.module}/sidecars/jaeger.yaml", {
      jaeger_collector_netloc = var.jaeger_collector_netloc
      jaeger_map_name         = kubernetes_config_map.jaeger_data.metadata[0].name
      jaeger_map_path         = local.jaeger_map_path
      docker_registry         = var.docker_registry_repo
    })
    "jaeger_nginx" = templatefile("${path.module}/sidecars/jaeger_nginx.yaml", {
      jaeger_collector_netloc = var.jaeger_collector_netloc
      jaeger_map_name         = kubernetes_config_map.jaeger_data.metadata[0].name
      jaeger_map_path         = local.jaeger_map_path
      nginx_map_name          = kubernetes_config_map.nginx_data.metadata[0].name
      nginx_map_path          = local.nginx_map_path
      docker_registry         = var.docker_registry_repo
    })
  }

  depends_on = [
    kubernetes_namespace.sidecar_ns,
    kubernetes_config_map.jaeger_data,
    kubernetes_config_map.nginx_data
  ]
}
