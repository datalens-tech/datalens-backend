locals {
  locations     = [for sn in data.ycp_vpc_subnet.dl_back_network_subnets : { subnet_id : sn.id, zone : sn.zone_id }]
  ops_locations = [for sn in data.ycp_vpc_subnet.dl_back_ops_network_subnets : { subnet_id : sn.id, zone : sn.zone_id }]
}

data "ycp_vpc_network" "dl_back_network" {
  network_id = module.constants.env_data.network_id
}

data "ycp_vpc_network" "dl_back_ops_network" {
  network_id = module.constants.env_data.ops_network_id
}

data "ycp_vpc_subnet" "dl_back_network_subnets" {
  for_each  = toset(module.constants.env_data.subnet_ids)
  subnet_id = each.key
}

data "ycp_vpc_subnet" "dl_back_ops_network_subnets" {
  for_each  = toset(module.constants.env_data.ops_subnet_ids)
  subnet_id = each.key
}

data "yandex_dns_zone" "datalens" {
  dns_zone_id = module.constants.env_data.core_dns_zone_id
}

data "yandex_dns_zone" "cookie" {
  count       = module.constants.env_data.cookie_dns_zone_id == null ? 0 : 1
  dns_zone_id = module.constants.env_data.cookie_dns_zone_id
}
