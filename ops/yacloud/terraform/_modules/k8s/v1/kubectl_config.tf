locals {
  kubeconfig = templatefile("${path.module}/templates/kubeconfig.tpl", {
    kubeconfig_name     = var.cluster_name
    endpoint            = local.endpoint
    cluster_auth_base64 = base64encode(local.ca)
    yc_profile          = var.yc_profile
    bastion             = var.bastion.enable
  })
}

resource "local_file" "kubeconfig" {
  content              = local.kubeconfig
  filename             = "./.kubeconfig_${var.cluster_name}"
  file_permission      = "0600"
  directory_permission = "0755"
}
