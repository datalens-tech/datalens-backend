resource "yandex_dns_recordset" "backend" {
  zone_id = module.subinfra_data.core_dns_zone.id
  name    = var.backend_main_subdomain
  type    = "AAAA"
  ttl     = 600
  data    = [ycp_vpc_address.back_lb.ipv6_address[0].address]
}

resource "yandex_dns_recordset" "dls" {
  count   = module.constants.env_data.apps_to_run.dls ? 1 : 0
  zone_id = module.subinfra_data.core_dns_zone.id
  name    = "dls"
  type    = "AAAA"
  ttl     = 600
  data    = [ycp_vpc_address.back_lb.ipv6_address[0].address]
}

resource "yandex_dns_recordset" "upload" {
  count   = var.upload_config.create_fqdn ? 1 : 0
  zone_id = module.subinfra_data.cookie_dns_zone[0].id
  name    = "upload"
  type    = var.upload_config.external_access ? "A" : "AAAA"
  ttl     = 600
  data = (
    var.upload_config.external_access ?
    [ycp_vpc_address.upload_lb_v4[0].external_ipv4_address[0].address] :
    [ycp_vpc_address.upload_lb.ipv6_address[0].address]
  )
}

locals {
  back_lb_fqdn = format(
    "%s.%s",
    yandex_dns_recordset.backend.name, trimsuffix(module.subinfra_data.core_dns_zone.zone, ".")
  )
  dls_lb_fqdn = module.constants.env_data.apps_to_run.dls ? format(
    "%s.%s",
    yandex_dns_recordset.dls[0].name, trimsuffix(module.subinfra_data.core_dns_zone.zone, ".")
  ) : ""
  upload_lb_fqdn_created = var.upload_config.create_fqdn ? format(
    "%s.%s",
    yandex_dns_recordset.upload[0].name, trimsuffix(module.subinfra_data.cookie_dns_zone[0].zone, ".")
  ) : ""
  upload_lb_fqdn_list = compact(concat([local.upload_lb_fqdn_created], var.upload_config.additional_fqdns))
}

resource "ycp_certificatemanager_certificate_request" "backend" {
  name           = "backend"
  challenge_type = "CHALLENGE_TYPE_UNSPECIFIED"
  domains        = compact([local.back_lb_fqdn, local.dls_lb_fqdn])
  cert_provider  = "INTERNAL_CA"
}

resource "ycp_certificatemanager_certificate_request" "upload_ext" {
  count          = module.constants.env_data.apps_to_run.file_uploader ? 1 : 0
  name           = "upload"
  challenge_type = var.upload_config.cert_settings.challenge_type
  domains        = compact(local.upload_lb_fqdn_list)
  cert_provider  = var.upload_config.cert_settings.cert_type
}
