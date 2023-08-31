resource "yandex_logging_group" "this" {
  name             = "app-logs"
  retention_period = "${90 * 24}h"
}
