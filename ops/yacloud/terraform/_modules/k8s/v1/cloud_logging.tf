resource "ycp_logging_group" "mk8s-audit-logs" {
  count = var.enable_silo ? 1 : 0

  name             = "mk8s-audit-logs"
  description      = "Kubernetes audit logs"
  folder_id        = var.folder_id
  retention_period = "72h0m0s"

  data_stream = var.k8s_audit_security_stream

  lifecycle {
    ignore_changes = [cold_retention_period]
  }
}
