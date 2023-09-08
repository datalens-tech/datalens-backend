
target "base_os" {
  context  = "target_base_os"
  args = {
    BASE_IMG    = "${BASE_LINUX}"
  }
  tags = [
    "${CR_TAG_BASE_OS}"
  ]
}
