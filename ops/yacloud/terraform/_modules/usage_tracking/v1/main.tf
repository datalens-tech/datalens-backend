terraform {
  required_providers {
    ycp = {
      source = "terraform.storage.cloud-preprod.yandex.net/yandex-cloud/ycp"
    }
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 0.73"
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
  clickhouse_transfer_password = local.clickhouse_passwords[var.clickhouse_properties.transfer_user]
  kafka_user_vector_password   = local.kafka_passwords[var.kafka_properties.vector_user]
  kafka_user_transfer_password = local.kafka_passwords[var.kafka_properties.transfer_user]
}

locals {
  clickhouse_transfer_user = var.clickhouse_properties.transfer_user
  clickhouse_database      = "usage_tracking"
  clickhouse_table         = "usage_tracking"
}
