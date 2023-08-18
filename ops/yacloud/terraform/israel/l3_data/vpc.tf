resource "yandex_vpc_network" "samples" {
  name = "samples"
}

resource "yandex_vpc_subnet" "samples_il1-a" {
  network_id     = yandex_vpc_network.samples.id
  zone           = "il1-a"
  v4_cidr_blocks = ["10.2.0.0/16"]
}

resource "yandex_vpc_subnet" "samples_il1-b" {
  network_id     = yandex_vpc_network.samples.id
  zone           = "il1-b"
  v4_cidr_blocks = ["10.3.0.0/16"]
}
