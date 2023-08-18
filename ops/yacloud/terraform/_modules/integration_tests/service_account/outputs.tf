output "sa_data" {
  value = {
    id          = yandex_iam_service_account.integration-tests-sa.id
    name        = yandex_iam_service_account.integration-tests-sa.name
    key_id      = yandex_iam_service_account_key.integration-tests-sa-key.id
    public_key  = yandex_iam_service_account_key.integration-tests-sa-key.public_key
    private_key = yandex_iam_service_account_key.integration-tests-sa-key.private_key
  }
}
