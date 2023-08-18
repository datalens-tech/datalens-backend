# where db clusters are being created
variable "db_clusters_folder_id" {
  type = string
}

variable "network_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "env_name" {
  type = string
}

variable "bi_api_secret_id" {
  type = string
}

variable "k8s_namespace" {
  default = "integration-tests"
}

variable "iam_api_host" {
  type = string
}

variable "iam_as_host" {
  type = string
}

variable "iam_ss_host" {
  type = string
}

variable "iam_ts_host" {
  type = string
}

variable "rm_host" {
  type = string
}

variable "back_lb_fqdn" {
  type = string
}

variable "dls_lb_fqdn" {
  type = string
}

variable "upload_fqdn" {
  type = string
}

variable "cloud_api_endpoint" {
  type = string
}

variable "use_internal_ca" {
  type = bool
}

variable "dls_enabled" {
  type = bool
}

variable "eso_sa_key" {}

variable "pg_cluster_resource_preset_id" {
  type = string
}

variable "us_lb_main_base_url" {
  type = string
}

variable "sa_secrets_lockbox_id" {
  type = string
}