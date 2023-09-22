locals {
  folder_id = yandex_resourcemanager_folder.integration-tests.id

  sa_secrets = {
    for sa in(var.sa_data_list) : sa.name_suffix => module.service_account[sa.name_suffix].sa_data
  }

  integration_tests_secrets_content = jsonencode({
    "service_accounts" = local.sa_secrets
    "folder_id"        = local.folder_id
  })
}

resource "yandex_lockbox_secret" "integration_tests" {
  folder_id = yandex_resourcemanager_folder.integration-tests.id
  name      = "integration_tests_secrets"
}

resource "yandex_lockbox_secret_version" "integration_tests" {
  secret_id = yandex_lockbox_secret.integration_tests.id

  entries {
    key        = "integration_tests.json"
    text_value = local.integration_tests_secrets_content
  }
}