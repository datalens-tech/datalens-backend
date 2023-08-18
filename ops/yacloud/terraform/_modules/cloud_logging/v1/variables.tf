variable "folder_id" {
  type = string
}

variable "k8s_namespace" {
  type    = string
  default = "cloudlogging"
}

variable "k8s_cluster_id" {
  type = string
}

variable "cloud_api_endpoint" {
  type = string
}
