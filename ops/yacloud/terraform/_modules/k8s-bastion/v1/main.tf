resource "shell_script" "manage_bastion" {
  triggers = {
    run = var.bastion_enabled
  }

  lifecycle_commands {
    create = <<-CMD
    set -euo pipefail
    ycp --config-path ${var.config_path} --profile ${var.environment} k8s cluster update ${var.cluster_id} -r - <<EOF
    bastion:
      enabled: ${var.bastion_enabled}
    update_mask:
      paths:
        - bastion.enabled
    EOF
    CMD
    delete = ""
  }

  lifecycle {
    ignore_changes = [
      read_error,
      lifecycle_commands,
    ]
  }
}
