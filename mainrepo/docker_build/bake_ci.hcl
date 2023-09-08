
target "base_ci" {
  context  = "target_base_ci"
  args = {
    BASE_IMG    = "${CR_TAG_BASE_OS}"
  }
  tags = [
    "${CR_TAG_BASE_CI}"
  ]
}

