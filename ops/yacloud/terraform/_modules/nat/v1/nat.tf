resource "yandex_iam_service_account" "this" {
  name = "nat-sa"
}

resource "yandex_resourcemanager_folder_iam_member" "nat_admin" {
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.this.id}"
  role      = "compute.admin"
}

data "yandex_compute_image" "nat_instance_image" {
  family = "nat-instance-ubuntu"
}

resource "ycp_vpc_subnet" "this" {
  count = length(var.locations)

  name           = format("nat-%s", var.locations[count.index]["zone"])
  network_id     = var.network_id
  zone_id        = var.locations[count.index]["zone"]
  v4_cidr_blocks = var.nat_subnet_cidrs[count.index]
}

resource "ycp_vpc_address" "this" {
  count = length(var.locations)

  name     = format("nat-address-%s", var.locations[count.index]["zone"])
  reserved = true

  external_ipv4_address_spec {
    zone_id = var.locations[count.index]["zone"]

    requirements {
      hints = []
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "yandex_compute_instance" "this" {
  count = length(var.locations)

  name = format("nat-instance-%s", var.locations[count.index]["zone"])
  zone = var.locations[count.index]["zone"]

  platform_id = var.nat_instance_vm_config.platform_id

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.nat_instance_image.id
    }
  }
  network_interface {
    subnet_id      = ycp_vpc_subnet.this[count.index].id
    nat            = true
    nat_ip_address = ycp_vpc_address.this[count.index].external_ipv4_address[0].address
    ipv6           = false
  }
  resources {
    cores         = var.nat_instance_vm_config.cores
    memory        = var.nat_instance_vm_config.memory
    core_fraction = var.nat_instance_vm_config.core_fraction
  }

  service_account_id = yandex_iam_service_account.this.id

  depends_on = [yandex_resourcemanager_folder_iam_member.nat_admin]
}


# gotta be attached to subnets manually
resource "yandex_vpc_route_table" "this" {
  count = length(var.locations)

  name       = format("route_through_nat_instance-%s", var.locations[count.index]["zone"])
  network_id = var.network_id

  static_route {
    destination_prefix = "0.0.0.0/0"
    next_hop_address   = yandex_compute_instance.this[count.index].network_interface[0].ip_address
  }
}
