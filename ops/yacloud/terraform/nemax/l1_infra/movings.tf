moved {
  from = module.main.yandex_vpc_security_group.http_from_yandex_only
  to   = module.security_groups.yandex_vpc_security_group.http_from_yandex_only
}

moved {
  from = module.main.yandex_vpc_security_group.allow_all
  to   = module.security_groups.yandex_vpc_security_group.allow_all
}

moved {
  from = module.main.module.k8s.local_file.kubeconfig
  to   = module.k8s.local_file.kubeconfig
}

moved {
  from = module.main.module.k8s.yandex_iam_service_account.cluster
  to   = module.k8s.yandex_iam_service_account.cluster
}

moved {
  from = module.main.module.k8s.yandex_iam_service_account.node
  to   = module.k8s.yandex_iam_service_account.node
}

moved {
  from = module.main.module.k8s.yandex_kubernetes_cluster.this
  to   = module.k8s.yandex_kubernetes_cluster.this
}

moved {
  from = module.main.module.k8s.yandex_resourcemanager_folder_iam_member.cluster_editor
  to   = module.k8s.yandex_resourcemanager_folder_iam_member.cluster_editor
}

moved {
  from = module.main.module.k8s.yandex_resourcemanager_folder_iam_member.node_image_puller
  to   = module.k8s.yandex_resourcemanager_folder_iam_member.node_image_puller
}

moved {
  from = module.main.module.k8s.yandex_vpc_security_group.k8s_main
  to   = module.k8s.yandex_vpc_security_group.k8s_main
}

moved {
  from = module.main.module.k8s.yandex_vpc_security_group.k8s_master_whitelist
  to   = module.k8s.yandex_vpc_security_group.k8s_master_whitelist
}

moved {
  from = module.main.module.k8s.ycp_vpc_address.master_v6[0]
  to   = module.k8s.ycp_vpc_address.master_v6[0]
}

