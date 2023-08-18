variable "cloud_id" {
  type = string
}

variable "sa_data_list" {
  type = list(object({
    name_suffix = string
    folder_roles = list(string)
  }))
}
