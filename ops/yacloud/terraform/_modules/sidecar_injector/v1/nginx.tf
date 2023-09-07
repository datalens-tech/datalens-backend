resource "tls_private_key" "nginx_private_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "tls_self_signed_cert" "nginx_cert" {
  private_key_pem = tls_private_key.nginx_private_key.private_key_pem

  subject {
    common_name = "*.${var.apps_namespace}.svc.cluster.local"
  }

  validity_period_hours = 8760 //  365 days or 1 year

  allowed_uses = [
    "digital_signature",
    "key_encipherment",
    "server_auth",
    "client_auth"
  ]
}

resource "kubernetes_config_map" "nginx_data" {
  metadata {
    name      = "nginx-data"
    namespace = var.apps_namespace
  }
  data = {
    "nginx.conf" = templatefile("${path.module}/sidecars/nginx.conf", {
      nginx_keepalive_timeout         = 100
      nginx_listen_port               = var.app_tls_port
      nginx_cert_location             = "${local.nginx_map_path}/cert.pem"
      nginx_cert_private_key_location = "${local.nginx_map_path}/cert.key"
      nginx_backend_port              = 80
    })
    "cert.pem" = "${tls_self_signed_cert.nginx_cert.cert_pem}"
    "cert.key" = "${tls_private_key.nginx_private_key.private_key_pem}"
  }
}

resource "kubernetes_config_map" "nginx_data_v2" {
  metadata {
    name      = "nginx-data-v2"
    namespace = var.apps_namespace
  }
  data = {
    "nginx.conf" = templatefile("${path.module}/sidecars/nginx.conf", {
      nginx_keepalive_timeout         = 100
      nginx_listen_port               = var.app_tls_port
      nginx_cert_location             = "${local.nginx_map_path}/cert.pem"
      nginx_cert_private_key_location = "${local.nginx_map_path}/cert.key"
      nginx_backend_port              = 8080
    })
    "cert.pem" = "${tls_self_signed_cert.nginx_cert.cert_pem}"
    "cert.key" = "${tls_private_key.nginx_private_key.private_key_pem}"
  }
}
