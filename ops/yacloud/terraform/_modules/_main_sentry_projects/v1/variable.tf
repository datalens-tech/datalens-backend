variable "projects" {
  type = map(object({
    platform = string
  }))
}

variable "users" {
  type = list(string)
}
