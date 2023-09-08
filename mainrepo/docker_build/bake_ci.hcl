
target "base_ci" {
  context  = "target_base_ci"
  contexts = {
    metapkg = "target:dl_src_metapkg"
  }
  args = {
    BASE_IMG    = "${CR_TAG_BASE_OS}"
  }
}

