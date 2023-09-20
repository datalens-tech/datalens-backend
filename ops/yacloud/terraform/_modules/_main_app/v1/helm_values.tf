resource "local_file" "helm_values_bi_back" {
  filename = "helm_values_bi_backend_generated.yaml"
  content = templatefile("${path.module}/templates/helm_values.tpl", {
    content = yamlencode(merge(

      {
        cloud_api_endpoint = module.constants.env_data.cloud_api_endpoint
        secrets = (
          var.tf_managed_secrets ?
          {
            bi_backend_common_secret_id             = yandex_lockbox_secret.app_secret["common"].id
            bi_backend_bi_api_secret_id             = yandex_lockbox_secret.app_secret["bi_api"].id
            bi_backend_service_account_secret_id    = yandex_lockbox_secret.app_secret["service_account"].id
            bi_backend_file_uploader_secret_id      = yandex_lockbox_secret.app_secret["file_uploader"].id
            bi_backend_frozen_connectors_secret_id  = yandex_lockbox_secret.app_secret["frozen_connectors"].id
            bi_backend_service_connectors_secret_id = yandex_lockbox_secret.app_secret["service_connectors"].id
          } :
          var.secrets_map
        )
        node_selection_options = merge(
          {
            node_selector_key    = local.ng_discriminator_node_label
            taint_toleration_key = "dedicated"
          },
          {
            for srv, node_group in var.k8s_map_service_node_group :
            srv => {
              node_selector    = local.map_node_group_key_node_group_discriminator[node_group]
              taint_toleration = local.map_node_group_key_node_group_discriminator[node_group]
            }
          }
        )
        apps_to_run            = module.constants.env_data.apps_to_run
        enabled_features       = var.enabled_features
        app_resources          = var.app_resources
        use_internal_ca        = module.constants.use_internal_ca
        crypto_key_name        = var.crypto_key_name
        crypto_key_version     = var.crypto_key_version
        crypto_key_version_old = var.crypto_key_version_old
        endpoints = {
          us_host     = var.us_host
          rm_host     = var.rm_host
          iam_api     = var.iam_api_host
          iam_as      = var.iam_as_host
          iam_ss      = var.iam_ss_host
          iam_ts      = var.iam_ts_host
        }
        iam_basic_permission      = var.iam_basic_permission
        jaeger_svc_name_suffix    = var.jaeger_svc_name_suffix
        secure_reader_socket_path = var.secure_reader_socket_path
        mdb = {
          managed_network_enabled = var.mdb_settings.managed_network_enabled,
          domains                 = join(",", var.mdb_settings.domains)
          cname_domains           = join(",", var.mdb_settings.cname_domains)
          remap                   = var.mdb_settings.remap
        }
        tls_to_backends = var.tls_to_backends
        redis_caches = var.enabled_features.caches == true ? {
          hosts        = module.redis_caches[0].hosts
          port         = length(module.redis_caches[0].hosts) > 1 ? 26379 : 6379
          cluster_name = module.redis_caches[0].name
          mode         = length(module.redis_caches[0].hosts) > 1 ? "sentinel" : "single_host"
          tls_enabled  = module.redis_caches[0].tls_enabled
        } : null,
        redis_misc = module.constants.env_data.apps_to_run.file_uploader == true ? {
          hosts        = module.redis_misc[0].hosts
          port         = length(module.redis_misc[0].hosts) > 1 ? 26379 : 6379
          cluster_name = module.redis_misc[0].name
          mode         = length(module.redis_misc[0].hosts) > 1 ? "sentinel" : "single_host"
          tls_enabled  = module.redis_misc[0].tls_enabled
        } : null,
        frozen_connectors_data = var.enabled_features.frozen_connectors == true ? var.frozen_connectors_data : [],
        file_connector_config = module.constants.env_data.apps_to_run.file_uploader == true ? {
          s3_endpoint            = "https://${module.constants.env_data.s3_endpoint}"
          persistent_bucket_name = module.file_connector[0].persistent_bucket_name
          tmp_bucket_name        = module.file_connector[0].tmp_bucket_name
          ch_hosts               = module.file_connector[0].ch_hosts
          ch_username            = module.file_connector[0].ch_user
          cors_allowed_origins   = var.file_connector_config.cors_allowed_origins
          sa_key_secret_id       = module.file_connector[0].sa_key_secret_id
          ch_user_secret_id      = module.file_connector[0].ch_user_secret_id
        } : null,
        sample_hosts : var.sample_hosts,
        alb_configs                = local.alb_configs
        dataset_api_num_of_workers = var.dataset_api_num_of_workers
        app_tls_port               = module.constants.app_tls_port
        ipv4_deny                  = var.ipv4_deny
        ipv6_deny                  = var.ipv6_deny
        host_allow                 = var.host_allow
      },
    ))
  })
}
