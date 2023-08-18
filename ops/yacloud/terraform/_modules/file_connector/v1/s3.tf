module "constants" {
  source   = "../../_constants/v1"
  env_name = var.env_name
}


resource "yandex_kms_symmetric_key" "this" {
  count = var.use_s3_encryption ? 1 : 0

  name              = "file-connector-s3-key"
  default_algorithm = "AES_128"
  rotation_period   = "8760h" // equal to 1 year
}

resource "yandex_storage_bucket" "persistent" {
  access_key = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  secret_key = yandex_iam_service_account_static_access_key.sa_static_key.secret_key

  bucket   = module.constants.file_connector_persistent_bucket_name
  max_size = var.persistent_bucket_size

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

  lifecycle {
    ignore_changes = [
      lifecycle_rule
    ]
  }
}

resource "yandex_storage_bucket" "tmp" {
  access_key = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  secret_key = yandex_iam_service_account_static_access_key.sa_static_key.secret_key

  bucket   = module.constants.file_connector_tmp_bucket_name
  max_size = var.tmp_bucket_size

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

  lifecycle_rule {
    id      = "older-than-24h"
    enabled = true
    expiration {
      days = 1
    }
  }

  lifecycle_rule {
    id                                     = "mp-uploads"
    enabled                                = true
    abort_incomplete_multipart_upload_days = 1
  }
}
