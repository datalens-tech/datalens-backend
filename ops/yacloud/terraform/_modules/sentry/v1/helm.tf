locals {
  namespace = "sentry"
}

resource "kubernetes_namespace" "sentry_ns" {
  metadata {
    name = local.namespace
  }
}

resource "helm_release" "sentry" {
  name              = "sentry"
  chart             = "sentry"
  repository        = "https://sentry-kubernetes.github.io/charts"
  version           = "17.7.1"
  timeout           = 600
  wait              = false
  dependency_update = true

  namespace = local.namespace

  values = [
    templatefile(
      "${path.module}/templates/sentry_values.yaml",
      {
        sentry_email    = "robot-datalens-back@yandex-team.ru"
        sentry_password = random_password.sentry_user_password.result

        postgres_db_host  = yandex_mdb_postgresql_cluster.sentry.host[0].fqdn # FIXME
        postgres_db_name  = yandex_mdb_postgresql_database.sentry_db.name
        postgres_username = yandex_mdb_postgresql_user.sentry_user.name
        postgres_password = random_password.sentry_pg_password.result
      }
    )
  ]

  depends_on = [
    kubernetes_namespace.sentry_ns,
    yandex_mdb_postgresql_cluster.sentry,
    yandex_mdb_postgresql_user.sentry_user,
    yandex_mdb_postgresql_database.sentry_db,
  ]
}
