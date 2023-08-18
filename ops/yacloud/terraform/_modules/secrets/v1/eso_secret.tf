output "eso_sa_key" {
  value = {
    "authorized-key" = jsonencode({
      id                 = yandex_iam_service_account_key.this.id
      created_at         = yandex_iam_service_account_key.this.created_at
      key_algorithm      = yandex_iam_service_account_key.this.key_algorithm
      public_key         = yandex_iam_service_account_key.this.public_key
      private_key        = yandex_iam_service_account_key.this.private_key
      service_account_id = yandex_iam_service_account_key.this.service_account_id
    })
  }
}
