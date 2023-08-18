module "sidecar_injector" {
  source = "../../sidecar_injector/v1"

  jaeger_collector_netloc = var.jaeger_host
  internal_cert           = module.infra_data.internal_cert
  apps_namespace          = var.apps_namespace
  docker_registry_repo    = var.docker_registry_repo
  app_tls_port            = module.constants.app_tls_port
}
