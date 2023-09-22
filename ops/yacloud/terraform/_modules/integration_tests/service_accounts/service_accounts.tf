module "service_account" {
  source = "../service_account"
  for_each = {
    for index, sa in var.sa_data_list :
    sa.name_suffix => sa
  }

  sa_name      = "integration-tests-${each.key}"
  folder_id    = yandex_resourcemanager_folder.integration-tests.id
  folder_roles = each.value.folder_roles
}
