locals {
  env_name = "yc-preprod"
  ch_samples_hosts = [
    # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdb636es44gm87hucoip/view
    "sas-gvwzxfe1s83fmwex.db.yandex.net",
    "vla-wwc7qtot5u6hhcqc.db.yandex.net",
  ]
  ch_datalens_ext_data_hosts = [
    # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdbd60phvp3hvq7d3sq6
    "sas-t2pl126yki67ztly.db.yandex.net",
    "vla-1zwdkyy37cy8iz7f.db.yandex.net",
  ]
  ch_ext_data_hosts = [
    # https://console.cloud.yandex.ru/folders/b1gm5rgsft916g8kjhnd/managed-clickhouse/cluster/c9qte8l5lhb4vuadou1u
    "rc1a-lcdz5pcu5f40kcas.mdb.yandexcloud.net",
    "rc1b-tbmlr5cfyd96efi5.mdb.yandexcloud.net",
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

provider "ycp" {
  token       = var.iam_token
  cloud_id    = module.constants.env_data.cloud_id
  folder_id   = module.constants.env_data.folder_id
  ycp_profile = module.constants.env_data.ycp_profile
  environment = module.constants.env_data.ycp_environment
}

provider "yandex" {
  token            = var.iam_token
  cloud_id         = module.constants.env_data.cloud_id
  folder_id        = module.constants.env_data.folder_id
  endpoint         = module.constants.env_data.cloud_api_endpoint
  storage_endpoint = module.constants.env_data.s3_endpoint
}

terraform {
  required_providers {
    ycp = {
      source = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ycp"
    }
    yandex = {
      source = "yandex-cloud/yandex"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
    }
    helm = {
      source = "hashicorp/helm"
    }
  }
  required_version = ">= 0.73"

  backend "s3" {
    endpoint = "storage.cloud-preprod.yandex.net"
    bucket   = "dl-tf-state"
    region   = "us-east-1"
    key      = "datalens-app.tfstate"

    skip_region_validation      = true
    skip_credentials_validation = true
  }
}

data "external" "cilium_rules" {
  count = module.constants.env_data.k8s_use_cilium ? 1 : 0

  program = ["python3", "${path.module}/generate_cilium_list.py"]
}


module "main" {
  source = "../../_modules/_main_app/v1"

  env_name = local.env_name

  backend_main_subdomain = "back"

  k8s_node_groups = {
    bi_api = {
      size          = 2
      cores         = 4
      core_fraction = 100
      memory        = 8
      dedicated     = true
    }
    dls = {
      size          = 2
      cores         = 4
      core_fraction = 100
      memory        = 8
      dedicated     = true
    }
    rest = {
      size          = 5
      cores         = 4
      core_fraction = 100
      memory        = 12
      dedicated     = false
    }
  }
  enabled_features = {
    frozen_connectors  = true
    service_connectors = true
    caches             = true
    rqe_caches         = true
    mutation_caches    = true
    network_policy     = module.constants.env_data.k8s_use_cilium
    rootless_mode      = true
  }

  app_resources = {
    dataset_api = {
      cpu = "1"
      ram = "2Gi"
    }
    dataset_data_api = {
      cpu = "1"
      ram = "2Gi"
    }
    public_dataset_api = {
      cpu = "1"
      ram = "2Gi"
    }
    dataset_data_api_sec_embeds = {
      cpu = "1"
      ram = "2Gi"
    }
    rqe_dataset_api = {
      cpu = "250m"
      ram = "500Mi"
    }
    dls_api = {
      cpu = "500m"
      ram = "1Gi"
    }
    dls_tasks = {
      cpu = "250m"
      ram = "500Mi"
    }
    file_uploader_api = {
      cpu = "250m"
      ram = "500Mi"
    }
    file_uploader_worker = {
      cpu = "250m"
      ram = "500Mi"
    }
    file_secure_reader = {
      cpu = "250m"
      ram = "500Mi"
    }
  }

  k8s_map_service_node_group = {
    dataset_api                 = "bi_api"
    dataset_data_api            = "bi_api"
    public_dataset_api          = "bi_api"
    dataset_data_api_sec_embeds = "bi_api"
    rqe_dataset_api             = "bi_api"
    dls_api                     = "dls"
    dls_tasks                   = "dls"
    file_uploader_api           = "rest"
    file_uploader_worker        = "rest"
    file_secure_reader          = "rest"
  }

  secrets_map = {
    bi_backend_common_secret_id             = "fc34lbiuk6r92c80qnc2"
    bi_backend_bi_api_secret_id             = "fc3h34voqset5mm4u10m"
    bi_backend_frozen_connectors_secret_id  = "fc3vd9dr3ccmp196m9b3"
    bi_backend_service_connectors_secret_id = "fc3sphf760bfqfto4rjp"
    bi_backend_dls_secret_id                = "fc3e8uh7mrm767b02vjp"
    bi_backend_service_account_secret_id    = "fc37dn5m5c15m07koh7a"
    bi_backend_file_uploader_secret_id      = "fc3e3ehpnqt7821d6dep"
  }

  upload_config = {
    create_fqdn      = false
    additional_fqdns = ["upload.datalens-preprod.yandex.ru"]
    external_access  = false
    cert_settings = {
      cert_type      = "INTERNAL_CA"
      challenge_type = "CHALLENGE_TYPE_UNSPECIFIED"
    }
  }

  crypto_key_name        = "cloud_preprod"
  crypto_key_version     = "3"
  crypto_key_version_old = "2"

  us_host      = "https://us.datalens-front.cloud-preprod.yandex.net"
  rm_host      = "rm.private-api.cloud-preprod.yandex.net:4284"
  iam_api_host = "iam.private-api.cloud-preprod.yandex.net:4283"
  iam_as_host  = "as.private-api.cloud-preprod.yandex.net:4286"
  iam_ss_host  = "ss.private-api.cloud-preprod.yandex.net:8655"
  iam_ts_host  = "ts.private-api.cloud-preprod.yandex.net:4282"

  iam_basic_permission = "datalens.instances.use"

  jaeger_svc_name_suffix = "-testing"

  secure_reader_socket_path = "/var/sockets/reader.sock"

  mdb_settings = {
    managed_network_enabled = true
    domains                 = [".mdb.cloud-preprod.yandex.net", ".mdb.yandexcloud.net"]
    cname_domains           = [".rw.mdb.cloud-preprod.yandex.net", ".rw.mdb.yandexcloud.net"]
    remap = {
      ".mdb.cloud-preprod.yandex.net" : ".db.yandex.net",
      ".mdb.yandexcloud.net" : ".db.yandex.net"
    }
  }

  caches_redis_config = {
    preset            = "hm1.nano"
    disk_size         = 16
    disable_autopurge = true
    locations         = [module.subinfra_data.locations[0]] # ru-central1-b (locations: b, a, c)
    db                = 0
  }

  misc_redis_config = {
    preset            = "b2.nano"
    disk_size         = 4
    disable_autopurge = true
    locations         = [module.subinfra_data.locations[1]] # ru-central1-a
    db                = 0
  }

  frozen_connectors_data = [
    {
      connector_settings_key = "CH_FROZEN_BUMPY_ROADS"
      hosts                  = local.ch_ext_data_hosts
      port                   = 8443
      db_name                = "bumpy_roads"
      username               = "bumpy_roads_user_ro"
      use_managed_network    = false
      raw_sql_level          = "off"
      pass_db_query_to_user  = false
      allowed_tables         = ["slow_polygons"]
      subselect_templates    = [],
    },
    {
      connector_settings_key = "CH_FROZEN_COVID"
      hosts                  = local.ch_samples_hosts
      port                   = 8443
      db_name                = "covid_public"
      username               = "covid_public"
      use_managed_network    = false
      raw_sql_level          = "off"
      pass_db_query_to_user  = false
      allowed_tables = [
        "iso_data_daily",
        "iso_data_hourly_with_history",
        "marker_share_by_region",
        "marker_share_by_region_stat",
        "russia_stat",
        "world_stat",
      ]
      subselect_templates = [],
    },
    {
      connector_settings_key = "CH_FROZEN_DEMO"
      hosts                  = local.ch_samples_hosts
      port                   = 8443
      db_name                = "samples"
      username               = "samples_user"
      use_managed_network    = false
      raw_sql_level          = "dashsql"
      pass_db_query_to_user  = true
      allowed_tables = [
        "bike_routes",
        "MS_Clients",
        "MS_MOSCOW_GEO",
        "MS_Products",
        "MS_SalesFacts_up",
        "MS_Shops",
        "run_routes",
        "TreeSample",
      ]
      subselect_templates = [],
    },
    {
      connector_settings_key = "CH_FROZEN_DTP"
      hosts                  = local.ch_ext_data_hosts
      port                   = 8443
      db_name                = "dtp_mos"
      username               = "dtp_mos_user_ro"
      use_managed_network    = false
      raw_sql_level          = "off"
      pass_db_query_to_user  = false
      allowed_tables         = ["points", "regions"]
      subselect_templates    = [],
    },
    {
      connector_settings_key = "CH_FROZEN_GKH"
      hosts                  = local.ch_datalens_ext_data_hosts
      port                   = 8443
      db_name                = "gkh"
      username               = "gkh_user"
      use_managed_network    = false
      raw_sql_level          = "off"
      pass_db_query_to_user  = false
      allowed_tables         = ["GKH_Alarm"]
      subselect_templates    = [],
    },
    {
      connector_settings_key = "CH_FROZEN_SAMPLES"
      hosts                  = local.ch_samples_hosts
      port                   = 8443
      db_name                = "samples"
      username               = "samples_user"
      use_managed_network    = false
      raw_sql_level          = "off"
      pass_db_query_to_user  = true
      allowed_tables         = ["SampleSuperstore"]
      subselect_templates    = [],
    },
    {
      connector_settings_key = "CH_FROZEN_TRANSPARENCY"
      hosts                  = local.ch_ext_data_hosts
      port                   = 8443
      db_name                = "transparency_report"
      username               = "transparency_report_user_ro"
      use_managed_network    = false
      raw_sql_level          = "off"
      pass_db_query_to_user  = false
      allowed_tables         = ["summary_stats_direct_transparency_report", "charity_mono_direct_transparency_report"]
      subselect_templates    = [],
    },
    {
      connector_settings_key = "CH_FROZEN_WEATHER"
      hosts                  = local.ch_datalens_ext_data_hosts
      port                   = 8443
      db_name                = "weather"
      username               = "dl_user"
      use_managed_network    = false
      raw_sql_level          = "off"
      pass_db_query_to_user  = false
      allowed_tables         = ["weather_stats"]
      subselect_templates    = [],
    },
    {
      connector_settings_key = "CH_FROZEN_HORECA"
      hosts                  = local.ch_datalens_ext_data_hosts
      port                   = 8443
      db_name                = "horeca"
      username               = "horeca_user"
      use_managed_network    = false
      raw_sql_level          = "off"
      pass_db_query_to_user  = false
      allowed_tables         = ["MosRest"]
      subselect_templates    = [],
    },
  ]

  file_connector_config = {
    ch_config = {
      num       = 1
      preset    = "m3-c2-m16"
      version   = "23.3"
      locations = module.subinfra_data.locations
    }
    persistent_bucket_size = 32212254720 // 30 GB
    tmp_bucket_size        = 5368709120  // 5 GB
    cors_allowed_origins   = ["*"]
    use_s3_encryption      = true
  }

  sample_hosts = [
    "myt-g2ucdqpavskt6irw.db.yandex.net",
    "sas-1h1276u34g7nt0vx.db.yandex.net",

    # datalens_ext_data https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdbd60phvp3hvq7d3sq6/view
    "sas-t2pl126yki67ztly.db.yandex.net",
    "vla-1zwdkyy37cy8iz7f.db.yandex.net",
    "c-mdbd60phvp3hvq7d3sq6.rw.db.yandex.net",

    "c-mdbggsqlf0pf2rar6cck.rw.db.yandex.net",
    "c-mdb636es44gm87hucoip.rw.db.yandex.net",

    # samples https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdb636es44gm87hucoip/view
    "sas-gvwzxfe1s83fmwex.db.yandex.net",
    "vla-wwc7qtot5u6hhcqc.db.yandex.net",

    # Ychebnik https://st.yandex-team.ru/BI-2935
    "vla-ws8isxcg383rpugb.db.yandex.net",
    "sas-0vu6ols4s7prltlm.db.yandex.net",
    "man-c4i8hlp8hp0qkmfo.db.yandex.net",
  ]

  alb_controller_helm = {
    repository = "oci://cr.cloud.yandex.net/yc-marketplace/yandex-cloud/yc-alb-ingress"
    version    = "v0.1.23"
  }

  integration_tests_enabled                       = true
  integration_tests_pg_cluster_resource_preset_id = "b1.nano"
  integration_tests_sa_secrets_lockbox_id         = "fc3f89dd2iujolljp3ed"

  dataset_api_num_of_workers = 4

  ipv4_deny  = module.constants.env_data.k8s_use_cilium ? data.external.cilium_rules[0].result.ipv4_deny : ""
  ipv6_deny  = module.constants.env_data.k8s_use_cilium ? data.external.cilium_rules[0].result.ipv6_deny : ""
  host_allow = module.constants.env_data.k8s_use_cilium ? data.external.cilium_rules[0].result.host_allow : ""

  setup_nat  = false
  nat_config = null

  providers = {
    yandex = yandex
    ycp    = ycp
  }
}
