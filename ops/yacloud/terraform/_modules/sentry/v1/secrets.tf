resource "yandex_lockbox_secret" "sentry_user" {
  name = "sentry_initial_user"
}

resource "random_password" "sentry_user_password" {
  length  = 32
  special = false
}

resource "yandex_lockbox_secret_version" "sentry_user_sec_version" {
  secret_id = yandex_lockbox_secret.sentry_user.id
  entries {
    key        = "password"
    text_value = random_password.sentry_user_password.result
  }
}

resource "yandex_lockbox_secret" "pg_user" {
  name = "sentry_pg_user"
}

resource "random_password" "sentry_pg_password" {
  length  = 32
  special = false
}

resource "yandex_lockbox_secret_version" "sentry_pg_sec_version" {
  secret_id = yandex_lockbox_secret.pg_user.id
  entries {
    key        = "password"
    text_value = random_password.sentry_pg_password.result
  }
}
