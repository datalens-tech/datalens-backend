resource "ycp_vpc_address" "sentry_lb_v6_address" {
  name     = "sentry-lb-v6-address"
  reserved = true

  ipv6_address_spec {
    requirements {
      hints = ["yandex-only"] # Address will be available only from Yandex' network
    }
  }
}

data "yandex_dns_zone" "datalens" {
  dns_zone_id = var.dns_zone_id
}

resource "yandex_dns_recordset" "sentry" {
  zone_id = data.yandex_dns_zone.datalens.id
  name    = "sentry"
  type    = "AAAA"
  ttl     = 600
  data    = [ycp_vpc_address.sentry_lb_v6_address.ipv6_address[0].address]
}

locals {
  sentry_lb_fqdn = format(
    "%s.%s",
    yandex_dns_recordset.sentry.name, trimsuffix(data.yandex_dns_zone.datalens.zone, ".")
  )
}

resource "ycp_certificatemanager_certificate_request" "sentry_cert" {
  name           = "sentry"
  challenge_type = "CHALLENGE_TYPE_UNSPECIFIED"
  domains        = [local.sentry_lb_fqdn]
  cert_provider  = "INTERNAL_CA"
}

resource "kubernetes_manifest" "alb" {
  manifest = {
    apiVersion = "networking.k8s.io/v1"
    kind       = "Ingress"
    metadata = {
      namespace = local.namespace
      name      = "sentry-ingress"
      annotations = {
        "helm.sh/hook-weight"                     = "-3"
        "ingress.alb.yc.io/group-name"            = "sentry-ingress-group"
        "ingress.alb.yc.io/subnets"               = join(",", var.subnet_ids)
        "ingress.alb.yc.io/security-groups"       = var.alb_security_group_id
        "ingress.alb.yc.io/idle-timeout"          = "5s"
        "ingress.alb.yc.io/request-timeout"       = "5s"
        "ingress.alb.yc.io/external-ipv6-address" = ycp_vpc_address.sentry_lb_v6_address.ipv6_address[0].address
      }
    }
    spec = {
      tls = [
        {
          hosts      = [local.sentry_lb_fqdn]
          secretName = "yc-certmgr-cert-id-${ycp_certificatemanager_certificate_request.sentry_cert.id}"
        }
      ],
      rules = [
        {
          host = local.sentry_lb_fqdn
          http = {
            paths = [
              {
                path     = "/"
                pathType = "Prefix"
                backend = {
                  service = {
                    name = "sentry-web"
                    port = { number = 9000 }
                  }
                }
              }
            ]
          }
        }
      ]
    }
  }
  depends_on = [kubernetes_namespace.sentry_ns]
}
