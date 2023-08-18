module "secret_store" {
  source = "../../secret_store/v1"

  k8s_namespace = var.k8s_namespace
  env_name      = var.env_name
  eso_sa_key    = var.eso_sa_key
}

data "yandex_lockbox_secret_version" "bi_api" {
  secret_id = var.bi_api_secret_id
}

data "yandex_lockbox_secret_version" "sa_secrets" {
  secret_id = var.sa_secrets_lockbox_id
}

locals {
  bi_api_secrets_map = { for e in data.yandex_lockbox_secret_version.bi_api.entries : e.key => e.text_value }
  integration_tests_pg_1 = tomap({
    host      = data.external.pg_host_address.result.host
    host_fqdn = local.pg_host_fqdn
    port      = 6432
    #not configurable, see https://registry.tfpla.net/providers/yandex-cloud/yandex/latest/docs/data-sources/datasource_mdb_postgresql_cluster
    database = yandex_mdb_postgresql_database.integration_tests.name
    username = yandex_mdb_postgresql_user.integration_tests.name
    password = yandex_mdb_postgresql_user.integration_tests.password
  })
  service_accounts_data = jsondecode(
    [ for e in data.yandex_lockbox_secret_version.sa_secrets.entries : e.text_value ][0]
  )

  integration_tests_secrets_content = jsonencode(
    merge(
      {
        "postgres"    = local.integration_tests_pg_1
        "dls_api_key" = local.bi_api_secrets_map["DLS_API_KEY"]
      },
      local.service_accounts_data
    )
  )
}

resource "yandex_lockbox_secret" "integration_tests" {
  folder_id = var.db_clusters_folder_id
  name      = "integration_tests_secrets"
}

resource "yandex_lockbox_secret_version" "integration_tests" {
  secret_id = yandex_lockbox_secret.integration_tests.id

  entries {
    key        = "integration_tests.json"
    text_value = local.integration_tests_secrets_content
  }
}