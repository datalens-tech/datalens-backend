resource "yandex_vpc_network" "this" {
  name = "for-integration-tests"
}

resource "yandex_vpc_subnet" "this" {
  name           = "for-integration-tests"
  network_id     = yandex_vpc_network.this.id
  v4_cidr_blocks = ["10.10.10.0/24"]
  zone           = local.locations[1].zone
}