resource "random_password" "password" {
  count   = 2
  length  = 32
  special = false
}

locals {
  redis_caches_password = random_password.password[0].result
  redis_misc_password   = random_password.password[1].result
}

resource "yandex_lockbox_secret" "redis_passwords" {
  name                = "redis_passwords"
  kms_key_id          = module.secrets.kms_key_id
  folder_id           = module.constants.env_data.folder_id
  deletion_protection = false
}

resource "yandex_lockbox_secret_version" "redis_passwords_sec_version" {
  secret_id = yandex_lockbox_secret.redis_passwords.id
  entries {
    key        = "REDIS_CACHES_PASSWORD"
    text_value = local.redis_caches_password
  }
  entries {
    key        = "REDIS_MISC_PASSWORD"
    text_value = local.redis_misc_password
  }
}

resource "kubernetes_secret" "redis_passwords" {
  metadata {
    name      = "redis-passwords"
    namespace = var.k8s_namespace
  }
  data = {
    REDIS_CACHES_PASSWORD = local.redis_caches_password
    REDIS_MISC_PASSWORD   = local.redis_misc_password
  }
}

module "redis_caches" {
  count  = var.enabled_features.caches ? 1 : 0
  source = "../../redis/v1"

  name               = "caches"
  network_id         = module.constants.env_data.network_id
  redis_version      = "6.2"
  preset             = var.caches_redis_config.preset
  disk_size          = var.caches_redis_config.disk_size
  disable_autopurge  = var.caches_redis_config.disable_autopurge
  locations          = var.caches_redis_config.locations
  password           = local.redis_caches_password
  security_group_ids = [module.infra_data.secgroup_allow_all.id]

}

module "redis_misc" {
  count  = module.constants.env_data.apps_to_run.file_uploader ? 1 : 0 # TODO: maybe use enabled_features
  source = "../../redis/v1"

  name              = "misc"
  network_id        = module.constants.env_data.network_id
  redis_version     = "6.2"
  preset            = var.misc_redis_config.preset
  disk_size         = var.misc_redis_config.disk_size
  disable_autopurge = var.misc_redis_config.disable_autopurge
  locations         = var.misc_redis_config.locations
  password          = local.redis_misc_password
}
