
target "base_ci" {
  context  = "target_base_ci"
  contexts = {
    metapkg = "target:dl_src_metapkg"
  }
  args = {
    BASE_IMG    = "${CR_TAG_BASE_OS}"
  }
}

target "ci_with_src" {
  context = "target_ci_with_src"
  contexts = {
    dl_src_lib  = "target:dl_src_lib"
  }
  args = {
    BASE_IMG = "${CR_BASE_IMG}"
  }
}