variable "INTERNAL_CERT_DIR" {}
variable "INTERNAL_CERT_NAME" {}

variable "CERT_BUNDLE_DIR" {}
variable "CERT_BUNDLE_NAME" {}

target "bundle_cert" {
  contexts = {
    cert_dir = "${INTERNAL_CERT_DIR}"
  }
  dockerfile-inline = <<EOT
FROM ubuntu:latest AS build
RUN apt update && apt install -y ca-certificates
COPY --from=cert_dir ${INTERNAL_CERT_NAME} /usr/local/share/ca-certificates
RUN update-ca-certificates

FROM scratch AS copy-cert
COPY --from=build /etc/ssl/certs/ca-certificates.crt /${CERT_BUNDLE_NAME}

EOT
  output = ["type=local,dest=${CERT_BUNDLE_DIR}"]
}
