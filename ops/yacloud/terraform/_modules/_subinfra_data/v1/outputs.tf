output "locations" {
  value = local.locations
}

output "ops_locations" {
  value = local.ops_locations
}

output "v4_cidrs" {
  value = flatten([for sn in data.ycp_vpc_subnet.dl_back_network_subnets : sn.v4_cidr_blocks])
}

output "core_dns_zone" {
  value = data.yandex_dns_zone.datalens
}

output "cookie_dns_zone" {
  value = data.yandex_dns_zone.cookie
}
