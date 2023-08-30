module "sentry" {
  count = var.enable_sentry == true ? 1 : 0

  source = "../../sentry/v1"

  network_id            = module.constants.env_data.network_id
  subnet_ids            = module.constants.env_data.subnet_ids
  dns_zone_id           = module.constants.env_data.core_dns_zone_id
  alb_security_group_id = var.sentry_alb_security_group_id
  postgresql_config     = var.sentry_pg_config
  sentry_version        = var.sentry_version
}
