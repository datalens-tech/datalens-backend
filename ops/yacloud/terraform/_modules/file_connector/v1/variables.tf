variable "folder_id" {
  type = string
}

variable "env_name" {
  type = string
}

variable "network_id" {
  type = string
}

variable "ch_config" {
  type = object({
    num     = number
    version = string
    preset  = string
    locations = list(object({
      subnet_id = string
      zone      = string
    }))
  })
}

variable "persistent_bucket_size" {
  type    = number
  default = 1099511627776 // 1 TB
}

variable "tmp_bucket_size" {
  type    = number
  default = 268435456000 // 250 GB
}

variable "use_s3_encryption" {
  type    = bool
  default = true
}

variable "disable_autopurge" {
  type    = bool
  default = false
}
