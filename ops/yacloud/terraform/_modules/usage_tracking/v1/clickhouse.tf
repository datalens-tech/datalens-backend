resource "null_resource" "usage-tracking-table" {
  provisioner "local-exec" {
    command = "${path.module}/create_ch_table.sh"
    environment = {
      CH_USER      = local.clickhouse_transfer_user
      CH_PASSWORD  = local.clickhouse_transfer_password
      CH_HOST      = var.logs_clickhouse.host[0].fqdn
      CH_CLUSTER   = var.logs_clickhouse.id
      CH_DATABASE  = local.clickhouse_database
      CH_TABLE     = local.clickhouse_table
      CA_CERT_PATH = var.internal_cert_path
    }
  }
}
