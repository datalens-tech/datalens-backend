locals {
  env_name = "nemax"
  ch_samples_hosts = [
    # https://console.cloudil.co.il/folders/yc.datalens.backend-service-folder/managed-clickhouse/cluster/camjokosu4jtf3cl4mhh/hosts
    "il1a-ng354nnfmestov48.mdb.cloudil.com", # TODO
    "il1b-6g6k5ciff6ubqis0.mdb.cloudil.com",
  ]
}

module "constants" {
  source   = "../../_modules/_constants/v1"
  env_name = local.env_name
}

module "subinfra_data" {
  source   = "../../_modules/_subinfra_data/v1"
  env_name = local.env_name
}

data "external" "deny_dst" {
  count = module.constants.env_data.k8s_use_cilium ? 1 : 0

  program = ["python3", "${path.module}/generate_deny_list.py"]
}

module "main" {
  source = "../../_modules/_main_app/v1"

  env_name = local.env_name

  backend_main_subdomain = "main"

  k8s_node_groups = {
    bi_api = {
      size          = 5
      cores         = 8
      core_fraction = 100
      memory        = 32
      dedicated     = true
    }
    rest = {
      size          = 5
      cores         = 4
      core_fraction = 100
      memory        = 16
      dedicated     = false
    }
  }
  enabled_features = {
    frozen_connectors  = true
    service_connectors = false
    caches             = true
    rqe_caches         = false
    mutation_caches    = true
    network_policy     = module.constants.env_data.k8s_use_cilium
    rootless_mode      = true
  }

  app_resources = {
    dataset_api = {
      cpu = "2"
      ram = "6Gi"
    }
    dataset_data_api = {
      cpu = "4"
      ram = "8Gi"
    }
    rqe_dataset_api = {
      cpu = "250m"
      ram = "500Mi"
    }
  }


  k8s_map_service_node_group = {
    dataset_api      = "bi_api"
    dataset_data_api = "bi_api"
    rqe_dataset_api  = "bi_api"
  }

  secrets_map        = null
  tf_managed_secrets = true

  crypto_key_name    = "nemax"
  crypto_key_version = "1"

  us_host      = "https://us.datalens-front.nemax.nebiuscloud.net" # TODO
  rm_host      = "rm.private-api.nemax.nebiuscloud.net:14284"
  iam_api_host = "iam.private-api.nemax.nebiuscloud.net:14283"
  iam_as_host  = "as.private-api.nemax.nebiuscloud.net:14286"
  iam_ss_host  = "ss.private-api.nemax.nebiuscloud.net:18655"
  iam_ts_host  = "ts.private-api.nemax.nebiuscloud.net:14282"

  iam_basic_permission = "datalens.instances.use"

  mdb_settings = {
    managed_network_enabled = true
    domains                 = [".mdb.nemax.nebius.cloud"]
    cname_domains           = [".rw.mdb.nemax.nebius.cloud"]
    remap = {
      ".mdb.nemax.nebius.cloud" : ".mdb.nemax.nebiuscloud.net"
    }
  }

  tls_to_backends = true

  caches_redis_config = {
    preset            = "s3-c2-m8"
    disk_size         = 16
    disable_autopurge = false
    locations = [ # in order to keep the il1-a host untouched
      module.subinfra_data.locations[1],
      module.subinfra_data.locations[0],
      module.subinfra_data.locations[2],
    ]
    db = 0
  }

  misc_redis_config = {
    preset            = "s3-c2-m8"
    disk_size         = 16
    disable_autopurge = false
    locations = [ # in order to keep the il1-a host untouched
      module.subinfra_data.locations[1],
      module.subinfra_data.locations[0],
      module.subinfra_data.locations[2],
    ]
    db = 0
  }

  frozen_connectors_data = [
    {
      connector_settings_key = "CH_FROZEN_DEMO"
      hosts                  = local.ch_samples_hosts
      port                   = 8443
      db_name                = "samples"
      username               = "samples_user_ro"
      use_managed_network    = true
      raw_sql_level          = "dashsql"
      pass_db_query_to_user  = true
      allowed_tables         = ["managers", "orders", "plan_managers"]
      subselect_templates    = [],
    }
  ]

  file_connector_config = {
    ch_config = {
      num       = 2
      preset    = "m3-c2-m16"
      version   = "23.3"
      locations = module.subinfra_data.locations
    }
    cors_allowed_origins = [
      "https://datalens.nemax.nebius.com",
    ]
    persistent_bucket_size = 1099511627776 // 1 TB
    tmp_bucket_size        = 268435456000  // 250 GB
    use_s3_encryption      = false
  }

  upload_config = {
    create_fqdn      = true
    additional_fqdns = []
    external_access  = false
    cert_settings = {
      cert_type      = "INTERNAL_CA"
      challenge_type = "CHALLENGE_TYPE_UNSPECIFIED"
    }
  }

  integration_tests_enabled                       = false
  integration_tests_pg_cluster_resource_preset_id = "s3-c2-m8"
  integration_tests_sa_secrets_lockbox_id         = "bcnglnc9t3b8j95447gj"

  secure_reader_socket_path = ""

  dataset_api_num_of_workers = 20

  ipv4_deny = module.constants.env_data.k8s_use_cilium ? data.external.deny_dst[0].result.ipv4 : ""
  ipv6_deny = module.constants.env_data.k8s_use_cilium ? data.external.deny_dst[0].result.ipv6 : ""

  setup_nat  = false
  nat_config = null

  providers = {
    yandex = yandex
    ycp    = ycp
  }
}
