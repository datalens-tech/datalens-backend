resource "local_file" "helm_values_bi_back" {
  filename = "helm_values_bi_backend_generated.yaml"
  content = templatefile("${path.module}/templates/helm_values.tpl", {
    content = yamlencode({
      sentry_enabled = 1
      sentry_dsns = {
        for name, config in var.projects : name => "http://${sentry_key.dlback_app_key[name].public}@sentry-relay.sentry:3000/${sentry_project.dlback_app[name].internal_id}"
      }
    })
  })
}
