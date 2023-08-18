resource "yandex_vpc_security_group" "http_from_yandex_only" { # FIXME: not accepted by ALB
  name       = module.constants.secgroup_http_from_yandex_only_name
  network_id = module.constants.env_data.network_id

  dynamic "ingress" {
    for_each = toset([80, 443])
    content {
      protocol       = "TCP"
      description    = "incoming"
      v4_cidr_blocks = ["0.0.0.0/0"]
      v6_cidr_blocks = ["::/0"]
      port           = ingress.value
    }
  }

  ingress {
    protocol    = "TCP"
    description = "lb_healthcheck"
    v4_cidr_blocks = [
      "198.18.235.0/24",
      "198.18.248.0/24",
    ]
    port = 30080
  }

  egress {
    protocol       = "ANY"
    description    = "ANY out"
    v4_cidr_blocks = ["0.0.0.0/0"]
    v6_cidr_blocks = ["::/0"]
    from_port      = 0
    to_port        = 65535
  }
}

resource "yandex_vpc_security_group" "allow_all" {
  name       = module.constants.secgroup_allow_all_name
  network_id = module.constants.env_data.network_id

  ingress {
    protocol       = "ANY"
    description    = "ANY in"
    v4_cidr_blocks = ["0.0.0.0/0"]
    v6_cidr_blocks = ["::/0"]
    from_port      = 0
    to_port        = 65535
  }
  egress {
    protocol       = "ANY"
    description    = "ANY out"
    v4_cidr_blocks = ["0.0.0.0/0"]
    v6_cidr_blocks = ["::/0"]
    from_port      = 0
    to_port        = 65535
  }
}

