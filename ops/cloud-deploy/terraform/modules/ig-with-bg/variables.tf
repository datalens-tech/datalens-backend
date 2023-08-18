variable "folder_id" {
  description = "Folder ID"
  type = string
}
variable "name" {
  description = "Name"
  type = string
}
variable "description" {
  description = "Description"
  type = string
}
variable "instance_group_id" {
  description = "L7 target group ID"
  type = string
}
variable "use_tls" {
  description = "Flag to use TLS in backend group"
  type = bool
  default = true
}
# TODO FIX: Find way to validate value
variable "tls_class" {
  description = "TLS class 'high' | 'low'"
  type = string
  default = "high"
}
