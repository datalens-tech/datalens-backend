resource "yandex_kms_symmetric_key" "this" {
  count = var.use_s3_encryption ? 1 : 0

  name              = "cloud-logs-s3-key"
  default_algorithm = "AES_128"
  rotation_period   = "8760h" // equal to 1 year
}

resource "yandex_storage_bucket" "logs-bucket" {
  access_key            = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  secret_key            = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
  default_storage_class = "COLD"
  bucket                = "logs-bucket"
  dynamic "server_side_encryption_configuration" {
    for_each = var.use_s3_encryption ? [1] : []
    content {
      rule {
        apply_server_side_encryption_by_default {
          kms_master_key_id = yandex_kms_symmetric_key.this[0].id
          sse_algorithm     = "aws:kms"
        }
      }
    }
  }

}
