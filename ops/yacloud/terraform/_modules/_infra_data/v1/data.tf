data "yandex_vpc_security_group" "http_from_yandex_only" {
  name = module.constants.secgroup_http_from_yandex_only_name
}

data "yandex_vpc_security_group" "allow_all" {
  name = module.constants.secgroup_allow_all_name
}

data "yandex_kubernetes_cluster" "this" {
  name = module.constants.k8s_cluster_name
}

data "local_file" "internal_cert" {
  filename = local.internal_cert_path
}

locals {
  internal_cert_path = "${path.module}/../../_constants/v1/certs/${module.constants.env_data.internal_cert_name}"
  preferrable_address = (
    module.constants.env_data.k8s_use_ext_v6 ?
    "[${data.yandex_kubernetes_cluster.this.master[0].external_v6_address}]" :
    data.yandex_kubernetes_cluster.this.master[0].external_v4_address
  )
  bastion_address = (
    module.constants.env_data.k8s_use_bastion ?
    "${data.yandex_kubernetes_cluster.this.id}.${module.constants.env_data.bastion_endpoint_suffix}" :
    null
  )
  k8s_address = module.constants.env_data.k8s_use_bastion ? "${local.bastion_address}" : "https://${local.preferrable_address}"
}
