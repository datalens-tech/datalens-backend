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
}

module "constants" {
  source = "../../_constants/v1"

  env_name = var.env_name
}

locals {
  clickhouse_transfer_user   = var.clickhouse_properties.transfer_user
  clickhouse_log_devops_user = "devops"
  clickhouse_ut_ro_user      = "ut_ro"

  clickhouse_backend_logs_database   = "backend_logs"
  clickhouse_usage_tracking_database = "usage_tracking"
  clickhouse_backend_app_logs_table  = "app_logs"
  clickhouse_version                 = "22.8"
}

data "yandex_lockbox_secret_version" "clickhouse_passwords" {
  secret_id = var.clickhouse_properties.secret_id
}

data "yandex_lockbox_secret_version" "kafka_passwords" {
  secret_id = var.kafka_properties.secret_id
}

locals {
  clickhouse_passwords = {
    for entry in data.yandex_lockbox_secret_version.clickhouse_passwords.entries : entry.key => entry.text_value
  }
  kafka_passwords = {
    for entry in data.yandex_lockbox_secret_version.kafka_passwords.entries : entry.key => entry.text_value
  }
}

locals {
  clickhouse_log_devops_password = local.clickhouse_passwords[local.clickhouse_log_devops_user]
  clickhouse_transfer_password   = local.clickhouse_passwords[local.clickhouse_transfer_user]
  kafka_user_vector_password     = local.kafka_passwords[var.kafka_properties.vector_user]
  kafka_user_transfer_password   = local.kafka_passwords[var.kafka_properties.transfer_user]
}
