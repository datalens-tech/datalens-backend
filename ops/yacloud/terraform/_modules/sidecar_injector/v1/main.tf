resource "kubectl_manifest" "sidecar_injector" {
  yaml_body = templatefile("${path.module}/templates/deployment.yaml", {
    namespace       = local.namespace
    sa_name         = kubernetes_service_account.sidecar_sa.metadata[0].name
    docker_registry = var.docker_registry_repo
  })

  depends_on = [
    kubernetes_namespace.sidecar_ns,
    kubernetes_service_account.sidecar_sa,
    kubernetes_config_map.sidecars
  ]
}
