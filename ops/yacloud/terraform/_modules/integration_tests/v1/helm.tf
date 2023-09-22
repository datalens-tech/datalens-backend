locals {
  helm_values = merge(
    {
      integration_tests_secret_id = yandex_lockbox_secret.integration_tests.id
      backendBalancer = {
        host = "https://${var.back_lb_fqdn}"
      },
      dls_enabled = var.dls_enabled,
      dlsBalancer = {
        host = "https://${var.dls_lb_fqdn}"
      },
      usBalancer = {
        host = var.us_lb_main_base_url
      }
      endpoints = {
        iam_api_host = var.iam_api_host
        rm_host      = var.rm_host
        iam_as       = var.iam_as_host
        iam_ts       = var.iam_ts_host
      },
      cloud_api_endpoint = var.cloud_api_endpoint
      use_internal_ca    = var.use_internal_ca
    },
    var.upload_fqdn == null ? {} : {
      uploadBalancer = {
        uploadHost = "https://${var.upload_fqdn}"
        statusHost = "https://${var.back_lb_fqdn}/file-uploader"
      }
    },
    yamldecode(file("helm_values_integration_tests_manual.yaml"))
  )
}

resource "helm_release" "integration_tests" {
  name              = "integration-tests"
  chart             = "../../../helm/integration-tests"
  timeout           = 600
  wait              = false
  dependency_update = true
  namespace         = var.k8s_namespace

  depends_on = [
    kubernetes_namespace.integration_tests
  ]

  values = [
    yamlencode(local.helm_values)
  ]
}
