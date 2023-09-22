variable "cluster_id" {
  type = string
}

variable "environment" {
  type = string
}

variable "bastion_enabled" {
  type = bool
}

variable "config_path" {
  type    = string
  default = "~/.config/ycp/config.yaml"
}
