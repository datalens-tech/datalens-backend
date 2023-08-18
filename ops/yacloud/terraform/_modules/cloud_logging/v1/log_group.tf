resource "yandex_logging_group" "this" {
  name             = "app-logs"
  retention_period = "72h"
}
