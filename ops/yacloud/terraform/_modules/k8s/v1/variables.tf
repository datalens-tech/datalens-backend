variable "k8s_version" {
  type = string
}

variable "yc_profile" {
  type        = string
  description = "yc-preprod / yc-prod"
}

variable "cluster_name" {
  type        = string
  description = "name of k8s cluster"
}

variable "cluster_region" {
  type = string
}

variable "network_id" {
  type        = string
  description = "network for k8s cluster"
}

variable "locations" {
  type = list(object({
    subnet_id = string
    zone      = string
  }))
  description = "list of az for k8s cluster"
}

variable "folder_id" {
  type        = string
  description = "folder for k8s cluster"
}

variable "service_ipv4_range" {
  type        = string
  description = "non overlapping range for pods, e.g. 10.97.0.0/16"
}

variable "service_ipv6_range" {
  type        = string
  description = "ipv6 range for pods"
}

variable "cluster_ipv4_range" {
  type        = string
  description = "non overlapping range for cluster nodes, e.g. 10.113.0.0/16"
}

variable "cluster_ipv6_range" {
  type        = string
  description = "IPV6 range for cluster nodes"
}

variable "security_groups_ids" {
  type        = list(string)
  description = "list of security groups"
}

variable "yandex_nets" {
  type = object({
    ipv4 = list(string)
    ipv6 = list(string)
  })
}

variable "healthchecks_cidrs" {
  type = object({
    v4 = list(string)
    v6 = list(string)
  })
}

variable "v4_cidrs" {
  type = list(string)
}

variable "use_ext_v6" {
  type = bool
}

variable "enable_silo" {
  type = bool
}

variable "use_cilium" {
  type = bool
}

variable "bastion" {
  type = object({
    enable          = bool
    cidr            = list(string)
    endpoint_suffix = string
  })
}

variable "k8s_audit_security_stream" {
  type        = string
  description = "YDS stream for K8S audit logs."
  default     = null
}
