locals {
  env_name = "israel"
  ch_samples_hosts = [
    # https://console.cloudil.co.il/folders/yc.datalens.backend-service-folder/managed-clickhouse/cluster/camjokosu4jtf3cl4mhh/hosts
    "il1a-ng354nnfmestov48.mdb.cloudil.com",
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
      size          = 4
      cores         = 8
      core_fraction = 100
      memory        = 24
      dedicated     = true
    }
    rest = {
      size          = 8
      cores         = 2
      core_fraction = 100
      memory        = 8
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
    file_uploader_api = {
      cpu = "1"
      ram = "2Gi"
    }
    file_uploader_worker = {
      cpu = "1"
      ram = "2Gi"
    }
    external_api = {
      cpu = "1"
      ram = "4Gi"
    }
    rqe_dataset_api = {
      cpu = "250m"
      ram = "500Mi"
    }
  }


  k8s_map_service_node_group = {
    dataset_api          = "bi_api"
    dataset_data_api     = "bi_api"
    file_uploader_api    = "rest"
    file_uploader_worker = "rest"
    external_api         = "rest"
    rqe_dataset_api      = "bi_api"
  }

  secrets_map = {
    bi_backend_common_secret_id             = "bcn6rmoh9fiaqfq2egja"
    bi_backend_bi_api_secret_id             = "bcn5439joer3stmh09ga"
    bi_backend_service_account_secret_id    = "bcncskkau760m3opb5s5"
    bi_backend_file_uploader_secret_id      = "bcnpm5t679ier3a24apk"
    bi_backend_frozen_connectors_secret_id  = "bcn8trhg5vvbqnbrjsnq"
    bi_backend_service_connectors_secret_id = "bcnvhn3ndn9cljln5m20"
  }

  crypto_key_name    = "israel"
  crypto_key_version = "1"

  us_host      = "https://us.datalens-front.yandexcloud.co.il"
  rm_host      = "rm.private-api.yandexcloud.co.il:14284"
  iam_api_host = "iam.private-api.yandexcloud.co.il:14283"
  iam_as_host  = "as.private-api.yandexcloud.co.il:14286"
  iam_ss_host  = "ss.private-api.yandexcloud.co.il:18655"
  iam_ts_host  = "ts.private-api.yandexcloud.co.il:14282"

  iam_basic_permission = "datalens.instances.use"

  mdb_settings = {
    managed_network_enabled = true
    domains                 = [".mdb.cloudil.com", ".mdb.il.nebius.cloud"]
    cname_domains           = [".rw.mdb.cloudil.com", ".rw.mdb.il.nebius.cloud"]
    remap = {
      ".mdb.cloudil.com" : ".mdb.yandexcloud.co.il"
      ".mdb.il.nebius.cloud" : ".mdb.yandexcloud.co.il"
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
      "https://datalens.cloudil.co.il",
      "https://datalens.il.nebius.com",
    ]
    persistent_bucket_size = 1099511627776 // 1 TB
    tmp_bucket_size        = 268435456000  // 250 GB
    use_s3_encryption      = false
  }

  upload_config = {
    create_fqdn      = true
    additional_fqdns = ["upload.datalens.cloudil.co.il"]
    external_access  = true
    cert_settings = {
      cert_type      = "LETS_ENCRYPT"
      challenge_type = "DNS"
    }
  }

  integration_tests_enabled                       = true
  integration_tests_pg_cluster_resource_preset_id = "s3-c2-m8"
  integration_tests_sa_secrets_lockbox_id         = "bcnglnc9t3b8j95447gj"

  secure_reader_socket_path = ""

  dataset_api_num_of_workers = 20

  ipv4_deny = module.constants.env_data.k8s_use_cilium ? data.external.deny_dst[0].result.ipv4 : ""
  ipv6_deny = module.constants.env_data.k8s_use_cilium ? data.external.deny_dst[0].result.ipv6 : ""

  setup_nat = false
  nat_config = {
    vm_config = {
      platform_id   = "standard-v3"
      cores         = 2
      core_fraction = 100
      memory        = 2
    }
    subnet_cidrs = [
      ["172.23.1.0/24"],
      ["172.23.2.0/24"],
      ["172.23.3.0/24"],
    ]
  }

  providers = {
    yandex = yandex
    ycp    = ycp
  }
}
