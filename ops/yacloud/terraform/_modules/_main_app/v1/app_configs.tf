module "app_configs" {
  count  = var.enabled_features.app_configs ? 1 : 0
  source = "../../app_configs/v1"
  k8s_namespace = var.k8s_namespace
}
