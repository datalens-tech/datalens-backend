resource "yandex_kms_symmetric_key" "this" {
  name              = "[TF] key for lockbox secrets"
  default_algorithm = "AES_128"
  rotation_period   = "8760h" // equal to 1 year
}
