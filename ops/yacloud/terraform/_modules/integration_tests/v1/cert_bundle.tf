module "infra_data" {
  source   = "../../_infra_data/v1"
  env_name = var.env_name
}


resource "kubernetes_secret" "cert_bundle" {
  metadata {
    name      = "cert-bundle"
    namespace = var.k8s_namespace
  }
  data = { "cert" = module.infra_data.cert_bundle }
}
