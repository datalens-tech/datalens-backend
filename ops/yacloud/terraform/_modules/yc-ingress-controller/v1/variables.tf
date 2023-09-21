variable "cluster_id" {
  type        = string
  description = "k8s cluster id"
}

variable "folder_id" {
  type        = string
  description = "folder id"
}

variable "network_id" {}

variable "subnet_ids" {
  type        = list(string)
  description = "list of subnets where cluster is located"
}

variable "k8s_endpoint" {}

variable "k8s_ca" {}

variable "k8s_namespace" {
  default = "yc-alb-ingress"
}

variable "cloud_api_endpoint" {
  type = string
}

variable "use_internal_ca" {
  type    = bool
  default = false
}

variable "internal_cert" {
  type = string
}

variable "helm_repository" {
  type = string
}

variable "helm_version" {
  type = string
}
