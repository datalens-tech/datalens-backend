module "secrets" {
  source = "../../secrets/v1"

  folder_id = module.constants.env_data.folder_id

  depends_on = [kubernetes_namespace.app, yandex_kubernetes_node_group.this]
}

module "secret_store" {
  source = "../../secret_store/v1"

  env_name      = var.env_name
  eso_sa_key    = module.secrets.eso_sa_key
  k8s_namespace = var.k8s_namespace
}

moved {
  from = module.secrets.kubernetes_secret.eso_sa_key
  to   = module.secret_store.kubernetes_secret.eso_sa_key
}