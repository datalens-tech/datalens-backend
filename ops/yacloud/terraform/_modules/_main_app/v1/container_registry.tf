resource "yandex_container_registry" "datalens" {
  name = "datalens"
}

resource "yandex_resourcemanager_folder_iam_member" "aw_sa_registry" {
  for_each = (
    var.aw_service_account_id == null ?
    [] : toset(["container-registry.images.puller", "container-registry.images.pusher"])
  )

  role      = each.value
  folder_id = module.constants.env_data.folder_id
  member    = "serviceAccount:${var.aw_service_account_id}"
}
