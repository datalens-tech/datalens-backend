resource "tls_private_key" "ca_private_key" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "tls_self_signed_cert" "sci_ca_cert" {
  private_key_pem = tls_private_key.ca_private_key.private_key_pem

  is_ca_certificate = true

  subject {
    common_name = "sidecar-injector-CA"
  }

  validity_period_hours = 43800 //  1825 days or 5 years

  allowed_uses = [
    "digital_signature",
    "key_encipherment",
    "cert_signing",
    "server_auth",
    "client_auth"
  ]
}

resource "tls_private_key" "int_private_key" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "tls_cert_request" "sci_internal_csr" {
  private_key_pem = tls_private_key.int_private_key.private_key_pem

  dns_names = [
    "sidecar-injector-prod",
    "sidecar-injector-prod.${local.namespace}",
    "sidecar-injector-prod.${local.namespace}.svc",
  ]

  subject {
    common_name = "sidecar-injector"
  }
}

resource "tls_locally_signed_cert" "sci_cert" {
  cert_request_pem   = tls_cert_request.sci_internal_csr.cert_request_pem
  ca_private_key_pem = tls_private_key.ca_private_key.private_key_pem
  ca_cert_pem        = tls_self_signed_cert.sci_ca_cert.cert_pem

  validity_period_hours = 43800

  allowed_uses = [
    "digital_signature",
    "key_encipherment",
    "server_auth",
    "client_auth",
  ]
}

resource "kubernetes_secret" "sidecar_injector_ca_cert" {
  metadata {
    name      = "sidecar-injector-ca-cert"
    namespace = local.namespace
  }

  data = {
    "tls.crt" = "${tls_self_signed_cert.sci_ca_cert.cert_pem}"
    "tls.key" = "${tls_private_key.ca_private_key.private_key_pem}"
  }

  type = "kubernetes.io/tls"
  depends_on = [
    kubernetes_namespace.sidecar_ns
  ]
}

resource "kubernetes_secret" "sidecar_injector_cert" {
  metadata {
    name      = "sidecar-injector-cert"
    namespace = local.namespace
  }

  data = {
    "tls.crt" = "${tls_locally_signed_cert.sci_cert.cert_pem}"
    "tls.key" = "${tls_private_key.int_private_key.private_key_pem}"
  }

  type = "kubernetes.io/tls"
  depends_on = [
    kubernetes_namespace.sidecar_ns
  ]
}
