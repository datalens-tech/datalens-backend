module "integration_tests" {
  count = var.integration_tests_enabled ? 1 : 0

  source = "../../integration_tests/v1"

  env_name = var.env_name

  back_lb_fqdn = local.back_lb_fqdn
  dls_lb_fqdn  = local.dls_lb_fqdn
  upload_fqdn  = local.upload_lb_fqdn_list[0]

  rm_host      = var.rm_host
  iam_api_host = var.iam_api_host
  iam_as_host  = var.iam_as_host
  iam_ss_host  = var.iam_ss_host
  iam_ts_host  = var.iam_ts_host

  cloud_api_endpoint = module.constants.env_data.cloud_api_endpoint

  providers = {
    kubernetes = kubernetes
    helm       = helm
    yandex     = yandex
    ycp        = ycp
  }

  use_internal_ca = module.constants.use_internal_ca
  dls_enabled     = module.constants.env_data.apps_to_run.dls

  eso_sa_key                 = module.secrets.eso_sa_key
  db_clusters_folder_id      = module.constants.env_data.folder_id
  bi_api_secret_id           = (
    var.tf_managed_secrets ?
    yandex_lockbox_secret.app_secret["bi_api"].id : var.secrets_map["bi_backend_bi_api_secret_id"]
  )

  network_id = module.constants.env_data.network_id
  subnet_ids = module.constants.env_data.subnet_ids

  pg_cluster_resource_preset_id = var.integration_tests_pg_cluster_resource_preset_id

  us_lb_main_base_url = var.us_host

  sa_secrets_lockbox_id =  var.integration_tests_sa_secrets_lockbox_id
}
