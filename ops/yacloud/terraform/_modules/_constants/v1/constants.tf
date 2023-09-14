variable "data" {
  type = map(object({
    env_name                    = string
    cloud_id                    = string
    folder_id                   = string
    integration_tests_folder_id = string
    network_id                  = string
    subnet_ids                  = list(string)
    ops_network_id              = string
    ops_subnet_ids              = list(string)
    cloud_api_endpoint          = string
    core_dns_zone_id            = string
    public_dns_zone_id          = string
    cookie_dns_zone_id          = string
    s3_endpoint                 = string
    main_zone_idx               = number # TODO: get rid of me
    secret_ids                  = map(string)
    ycp_profile                 = string
    ycp_environment             = string
    k8s_use_ext_v6              = bool
    k8s_use_cilium              = bool
    k8s_use_bastion             = bool
    bastion_cidr                = list(string)
    bastion_endpoint_suffix     = string
    k8s_audit_security_stream   = string
    apps_to_run = object({
      bi_api        = bool
      bi_api_public = bool
      sec_embeds    = bool
      file_uploader = bool
      dls           = bool
      secure_reader = bool
    })
  }))

  # TODO: convince IDEA that's a proper value
  default = {
    "yc-preprod" = {
      env_name                    = "yc-preprod"
      cloud_id                    = "aoee4gvsepbo0ah4i2j6"
      folder_id                   = "aoevv1b69su5144mlro3"
      integration_tests_folder_id = "aoe4ht5p1r9v2l9r01qq"
      network_id                  = "c64an5jmjnpgiaclqg8k"
      subnet_ids = [
        "buct4cp7lc8p0p2rht5l",
        "bltpp0md5jja3tr5tt9b",
        "fo25b29t13usc4529u5b",
      ]
      # we don't have ops network in yc-preprod
      # so use backend_network for the same reasons
      ops_network_id = "c64an5jmjnpgiaclqg8k"
      ops_subnet_ids = [
        "buct4cp7lc8p0p2rht5l",
        "bltpp0md5jja3tr5tt9b",
        "fo25b29t13usc4529u5b",
      ]
      cloud_api_endpoint = "api.cloud-preprod.yandex.net:443"
      core_dns_zone_id   = "aet6mjggkdfuhers6kd4"
      public_dns_zone_id = null
      cookie_dns_zone_id = null
      s3_endpoint        = "storage.cloud-preprod.yandex.net"
      main_zone_idx      = 0
      secret_ids = {
        kafka_passwords = "fc3fj14hndj3cao08nak"
      }
      ycp_profile        = "yc-preprod"
      ycp_environment    = "preprod"
      k8s_use_ext_v6     = true
      k8s_use_cilium     = true
      k8s_use_bastion    = true
      bastion_cidr = [
        "2a02:6b8:c03:501:0:fca0::/112",
        "2a02:6b8:c0e:501:0:fca0::/112",
        "2a02:6b8:c02:901:0:fca0::/112",
      ]
      bastion_endpoint_suffix   = "k8s.bastion.cloud-preprod.yandex.net"
      k8s_audit_security_stream = "/pre-prod_ydb_public/yc.security.infrastructure/cc88he6e60ecdr8onsb4/mk8s-audit-logs-topic"
      apps_to_run = {
        bi_api        = true
        bi_api_public = true
        sec_embeds    = true
        file_uploader = true
        dls           = true
        secure_reader = true
      }
    },
    "israel" = {
      env_name                    = "israel"
      cloud_id                    = "yc.datalens.backend-service-cloud"
      folder_id                   = "yc.datalens.backend-service-folder"
      integration_tests_folder_id = "yc.datalens.backend-service-folder"
      network_id                  = "ccmbej8t17vft19lu30u"
      subnet_ids = [
        "ddk2rck6hasf82hj7tqv",
        "dqpt7i3npraj4o3nu8lf",
        "c4ioees7ph151ltpqqdr",
      ]
      ops_network_id = "ccm1n0bt0jsbutofq91f"
      ops_subnet_ids = [
        "ddknqcofs1ujt5uefqn2",
        "c4i74sjd6vi9llf1qrj4",
        "dqpbglrt5ojntg2cv4fg",
      ]
      cloud_api_endpoint = "api.il.nebius.cloud:443"
      core_dns_zone_id   = "yc.datalens.backend"
      public_dns_zone_id = "b441fffa2f4b8cn6qpf6"
      cookie_dns_zone_id = "yc.datalens.backend_cookie"
      s3_endpoint        = "storage-internal.il.nebius.cloud"
      main_zone_idx      = 1
      secret_ids = {
        kafka_passwords = "bcn78c2ahhevf6m4ephk"
      }
      ycp_profile        = "israel"
      ycp_environment    = "israel"
      k8s_use_ext_v6     = true
      k8s_use_cilium     = false
      k8s_use_bastion    = false
      bastion_cidr       = []
      bastion_endpoint_suffix   = "k8s.bastion.yandexcloud.co.il"
      k8s_audit_security_stream = null
      apps_to_run = {
        bi_api        = true
        bi_api_public = false
        sec_embeds    = false
        file_uploader = true
        dls           = false
        secure_reader = false
      }
    },
    "nemax" = {
      env_name                    = "nemax"
      cloud_id                    = "yc.datalens.backend-service-cloud"
      folder_id                   = "yc.datalens.backend-service-folder"
      integration_tests_folder_id = "yc.datalens.backend-service-folder"
      network_id                  = "btcs90p05hibo5mh11na"
      subnet_ids = [
        "dc767d8e5ta09gnrg781",
        "dmaksg2qb59b5ajcj7d3",
        "f8ut55lejfd7a2acssvf",
      ]
      ops_network_id = "btceuge29d4pevsjsji7"
      ops_subnet_ids = [
        "dc7uf6ffesobi7md8rd5",
        "dmailb60og22fl8hjvh2",
        "f8u60b2emiajalnhsrlp",
      ]
      cloud_api_endpoint = "api.nemax.nebius.cloud:443"
      core_dns_zone_id   = "yc.datalens.backend"
      public_dns_zone_id = "b3q7shgiq8cdlsvocefc"
      cookie_dns_zone_id = "yc.datalens.backend_cookie"
      s3_endpoint        = "storage-internal.nemax.nebius.cloud"
      main_zone_idx      = 1
      secret_ids = {}
      ycp_profile        = "nemax"
      ycp_environment    = "nemax"
      k8s_use_ext_v6     = true
      k8s_use_cilium     = true
      k8s_use_bastion    = false
      bastion_cidr       = []
      bastion_endpoint_suffix   = "k8s.bastion.nemax.nebiuscloud.net"
      k8s_audit_security_stream = null
      apps_to_run = {
        bi_api        = true
        bi_api_public = false
        sec_embeds    = false
        file_uploader = false
        dls           = false
        secure_reader = false
      }
    }
  }
}

locals {
  users = [
    "seray@yandex-team.ru",
    "vgol@yandex-team.ru",
    "thenno@yandex-team.ru",
    "konstasa@yandex-team.ru",
    "mcpn@yandex-team.ru",
    "asnytin@yandex-team.ru",
    "gstatsenko@yandex-team.ru",
    "alex-ushakov@yandex-team.ru",
    "dmifedorov@yandex-team.ru",
    "kchupin@yandex-team.ru",
  ]

  yandex_cidrs = {
    ipv4 = [
      "5.45.192.0/18",
      "5.255.192.0/18",
      "37.9.64.0/18",
      "37.140.128.0/18",
      "45.87.132.0/22",
      "77.88.0.0/18",
      "84.252.160.0/19",
      "87.250.224.0/19",
      "90.156.176.0/22",
      "93.158.128.0/18",
      "95.108.128.0/17",
      "100.43.64.0/19",
      "139.45.249.96/29", // https://st.yandex-team.ru/NOCREQUESTS-25161
      "141.8.128.0/18",
      "178.154.128.0/18",
      "185.32.187.0/24",
      "199.21.96.0/22",
      "199.36.240.0/22",
      "213.180.192.0/19",
    ]

    ipv6 = [
      "2620:10f:d000::/44",
      "2a02:6b8::/32",
    ]
  }

  logs_clickhouse_users = {
    transfer_user_name = "transfer"
  }

  kafka_users = {
    kafka_user_vector_name   = "vector"
    kafka_user_transfer_name = "datatransfer"
  }

  kafka_topics = {
    k8s_logs_topic       = "app-k8s-logs"
    parsed_logs_topic    = "app-parsed-logs"
    filtered_logs_topic  = "app-filtered-logs"
    unparsed_logs_topic  = "app-unparsed-logs"
    usage_tracking_topic = "app-usage-tracking"
  }

  app_tls_port = 8443
}
