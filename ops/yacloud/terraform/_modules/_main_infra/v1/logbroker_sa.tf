data "yandex_client_config" "client" {}

resource "null_resource" "grant_logbroker_perm_for_node_sa" {
  for_each = var.setup_logbroker ? toset(["debug", "fast"]) : toset([])

  provisioner "local-exec" {
    command = join(" ", [
      "ya", "tool", "logbroker", "-s", var.logbroker_name,
      "permissions", "grant", "--path", "${module.constants.env_data.cloud_id}/${each.key}",
      "--subject", "${module.k8s.k8s_ng_sa_id}@as",
      "--permissions", "WriteTopic",
      "-y"
    ])
    environment = {
      IAM_TOKEN = data.yandex_client_config.client.iam_token
    }
  }
}
