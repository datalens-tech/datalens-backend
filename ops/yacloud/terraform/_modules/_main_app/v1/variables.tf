variable "env_name" {
  type = string
}

variable "backend_main_subdomain" {
  type = string
}

variable "k8s_node_groups" {
  type = map(object({
    cores         = number
    memory        = number
    core_fraction = number
    size          = number
    dedicated     = bool
  }))
}

variable "k8s_map_service_node_group" {
  type = map(string)
}

variable "enabled_features" {
  type = object({
    frozen_connectors  = bool
    service_connectors = bool
    caches             = bool
    rqe_caches         = bool
    mutation_caches    = bool
    network_policy     = bool
    rootless_mode      = bool
  })
}

variable "tf_managed_secrets" {
  type    = bool
  default = false
}

variable "secrets_map" {
  nullable = true
}

variable "k8s_namespace" {
  default = "bi-backend"
}

variable "crypto_key_name" {
  type = string
}

variable "crypto_key_version" {
  type = string
}

variable "crypto_key_version_old" {
  type    = string
  default = "0"
}

variable "us_host" {
  type = string
}

variable "iam_api_host" {
  type = string
}

variable "iam_as_host" {
  type = string
}

variable "iam_ss_host" {
  type = string
}

variable "iam_ts_host" {
  type = string
}

variable "iam_basic_permission" {
  type = string
}

variable "rm_host" {
  type = string
}

variable "mdb_settings" {
  type = object({
    managed_network_enabled = bool
    domains                 = list(string)
    cname_domains           = list(string)
    remap                   = map(string)
  })
}

variable "app_resources" {
  type = map(object({
    cpu = string
    ram = string
  }))
}

variable "tls_to_backends" {
  type    = bool
  default = true
}

variable "caches_redis_config" {
  type = object({
    preset            = string
    disk_size         = number
    disable_autopurge = bool
    locations         = list(map(string))
    db                = number
  })
  default = {
    preset            = "hm1.nano"
    disk_size         = 16
    disable_autopurge = true
    locations         = []
    db                = 0
  }
}

variable "misc_redis_config" {
  type = object({
    preset            = string
    disk_size         = number
    disable_autopurge = bool
    locations         = list(map(string))
    db                = number
  })
  default = {
    preset            = "hm1.nano"
    disk_size         = 16
    disable_autopurge = true
    locations         = []
    db                = 0
  }
}

variable "file_connector_config" {
  type = object({
    ch_config = object({
      num     = number
      version = string
      preset  = string
      locations = list(object({
        subnet_id = string
        zone      = string
      }))
    })
    tmp_bucket_size        = number
    persistent_bucket_size = number
    cors_allowed_origins   = list(string)
    use_s3_encryption      = bool
  })
}

variable "upload_config" {
  type = object({
    create_fqdn      = bool
    additional_fqdns = list(string)
    external_access  = bool
    cert_settings = object({
      cert_type      = string
      challenge_type = string
    })
  })
}

variable "frozen_connectors_data" {
  type = list(object({
    connector_settings_key = string
    hosts                  = list(string)
    port                   = number
    db_name                = string
    username               = string
    use_managed_network    = bool
    raw_sql_level          = optional(string)
    pass_db_query_to_user  = optional(bool)
    allowed_tables         = list(string)
    subselect_templates = list(object({
      title     = string
      sql_query = string
    }))
  }))
  default = []
}

variable "sample_hosts" {
  type    = list(string)
  default = []
}

variable "integration_tests_enabled" {
  type = bool
}

variable "integration_tests_pg_cluster_resource_preset_id" {
  type = string
}

variable "secure_reader_socket_path" {
  type = string
}

variable "dataset_api_num_of_workers" {
  type    = number
  default = 40
}

variable "integration_tests_sa_secrets_lockbox_id" {
  type = string
}

variable "ipv4_deny" {
  type    = string
  default = ""
}

variable "ipv6_deny" {
  type    = string
  default = ""
}

variable "host_allow" {
  type    = string
  default = ""
}

variable "setup_nat" {
  type    = bool
  default = false
}

variable "nat_config" {
  type = object({
    vm_config = object({
      platform_id   = string
      cores         = number
      core_fraction = number
      memory        = number
    })
    subnet_cidrs = list(list(string))
  })

  nullable = true
}
