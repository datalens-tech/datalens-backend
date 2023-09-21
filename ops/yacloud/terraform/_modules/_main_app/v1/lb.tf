module "yc-ingress-controller" {
  source = "../../yc-ingress-controller/v1"

  cluster_id         = module.infra_data.k8s_cluster.id
  folder_id          = module.constants.env_data.folder_id
  network_id         = module.constants.env_data.network_id
  subnet_ids         = module.constants.env_data.subnet_ids
  k8s_endpoint       = module.infra_data.k8s_endpoint
  k8s_ca             = module.infra_data.k8s_cluster.master[0].cluster_ca_certificate
  cloud_api_endpoint = module.constants.env_data.cloud_api_endpoint
  use_internal_ca    = module.constants.use_internal_ca
  internal_cert      = module.infra_data.internal_cert
  helm_repository    = var.alb_controller_helm.repository
  helm_version       = var.alb_controller_helm.version

  depends_on = [yandex_kubernetes_node_group.this]
}

resource "ycp_vpc_address" "back_lb" {
  name     = "back-lb-v6-address"
  reserved = true

  ipv6_address_spec {
    requirements {
      hints = ["yandex-only"] # Address will be available only from Yandex' network
    }
  }
}

resource "ycp_vpc_address" "upload_lb_v4" {
  count = var.upload_config.external_access ? 1 : 0

  name     = "upload-lb-v4-address"
  reserved = true

  external_ipv4_address_spec {
    zone_id = module.subinfra_data.locations[module.constants.env_data.main_zone_idx]["zone"]

    requirements {
      hints = []
    }
  }

  lifecycle {
    prevent_destroy = false
  }
}

resource "ycp_vpc_address" "upload_lb" {
  name     = "upload-lb-v6-address"
  reserved = true

  ipv6_address_spec {
    requirements {
      hints = var.upload_config.external_access ? [] : ["yandex-only"]
    }
  }
}

locals {
  container_port = var.tls_to_backends ? 443 : 80

  alb_configs = merge(
    {
      dataset_api = {
        name            = "dataset-api"
        ingress_group   = "back"
        subnets         = module.constants.env_data.subnet_ids
        security_groups = [module.infra_data.secgroup_allow_all.id]
        idle_timeout    = "95s"
        request_timeout = "95s"
        ipv6_address    = ycp_vpc_address.back_lb.ipv6_address[0].address
        certificate_id  = ycp_certificatemanager_certificate_request.backend.id
        hosts           = [local.back_lb_fqdn]
        paths = [
          {
            path         = "/api/v1"
            path_type    = "Prefix"
            service_name = "dataset-api-app"
            port         = local.container_port
          }
        ]
      }
      dataset_data_api = {
        name            = "dataset-data-api"
        ingress_group   = "back"
        subnets         = module.constants.env_data.subnet_ids
        security_groups = [module.infra_data.secgroup_allow_all.id]
        idle_timeout    = "95s"
        request_timeout = "95s"
        ipv6_address    = ycp_vpc_address.back_lb.ipv6_address[0].address
        certificate_id  = ycp_certificatemanager_certificate_request.backend.id
        hosts           = [local.back_lb_fqdn]
        paths = [
          {
            path         = "/api/data"
            path_type    = "Prefix"
            service_name = "dataset-data-api-app"
            port         = local.container_port
          }
        ]
      }
    },
    module.constants.env_data.apps_to_run.bi_api_public ? {
      public_dataset_api = {
        name            = "public-dataset-api"
        ingress_group   = "back"
        subnets         = module.constants.env_data.subnet_ids
        security_groups = [module.infra_data.secgroup_allow_all.id]
        idle_timeout    = "95s"
        request_timeout = "95s"
        prefix_rewrite  = "/api/data"
        ipv6_address    = ycp_vpc_address.back_lb.ipv6_address[0].address
        certificate_id  = ycp_certificatemanager_certificate_request.backend.id
        hosts           = [local.back_lb_fqdn]
        paths = [
          {
            path         = "/public/api/data"
            path_type    = "Prefix"
            service_name = "public-dataset-api-app"
            port         = local.container_port
          }
        ]
      }
    } : {},
    module.constants.env_data.apps_to_run.sec_embeds ? {
      dataset_data_api_sec_embeds = {
        name            = "dataset-data-api-sec-embeds"
        ingress_group   = "back"
        subnets         = module.constants.env_data.subnet_ids
        security_groups = [module.infra_data.secgroup_allow_all.id]
        idle_timeout    = "95s"
        request_timeout = "95s"
        prefix_rewrite  = "/api/data"
        ipv6_address    = ycp_vpc_address.back_lb.ipv6_address[0].address
        certificate_id  = ycp_certificatemanager_certificate_request.backend.id
        hosts           = [local.back_lb_fqdn]
        paths = [
          {
            path         = "/sec-embed/api/data"
            path_type    = "Prefix"
            service_name = "dataset-data-api-sec-embeds-app"
            port         = local.container_port
          }
        ]
      }
    } : {},
    module.constants.env_data.apps_to_run.dls ? {
      dls_api = {
        name            = "dls"
        ingress_group   = "back"
        subnets         = module.constants.env_data.subnet_ids
        security_groups = [module.infra_data.secgroup_allow_all.id]
        idle_timeout    = "10s"
        request_timeout = "10s"
        ipv6_address    = ycp_vpc_address.back_lb.ipv6_address[0].address
        certificate_id  = ycp_certificatemanager_certificate_request.backend.id
        hosts           = [local.dls_lb_fqdn]
        paths = [
          {
            path         = "/"
            path_type    = "Prefix"
            service_name = "dls-api-app"
            port         = local.container_port
          }
        ]
      }
    } : {},
    module.constants.env_data.apps_to_run.file_uploader ? {
      file_uploader_api = {
        name            = "file-uploader"
        ingress_group   = "back"
        subnets         = module.constants.env_data.subnet_ids
        security_groups = [module.infra_data.secgroup_allow_all.id]
        idle_timeout    = "30s"
        request_timeout = "30s"
        prefix_rewrite  = "/"
        ipv6_address    = ycp_vpc_address.back_lb.ipv6_address[0].address
        certificate_id  = ycp_certificatemanager_certificate_request.backend.id
        hosts           = [local.back_lb_fqdn]
        paths = [
          {
            path         = "/file-uploader/"
            path_type    = "Prefix"
            service_name = "file-uploader-api"
            port         = local.container_port
          }
        ]
      }
    } : {},
    module.constants.env_data.apps_to_run.file_uploader ? {
      upload = {
        name            = "upload"
        ingress_group   = "upload"
        subnets         = module.constants.env_data.subnet_ids
        security_groups = [module.infra_data.secgroup_allow_all.id]
        idle_timeout    = "305s"
        request_timeout = "305s"
        ipv4_address    = try(ycp_vpc_address.upload_lb_v4[0].external_ipv4_address[0].address, null)
        ipv6_address    = ycp_vpc_address.upload_lb.ipv6_address[0].address
        certificate_id  = ycp_certificatemanager_certificate_request.upload_ext[0].id
        hosts           = local.upload_lb_fqdn_list
        paths = [
          {
            path         = "/api/v2/files"
            path_type    = "Exact"
            service_name = "file-uploader-api"
            port         = local.container_port
          }
        ]
      }
    } : {}
  )
}
