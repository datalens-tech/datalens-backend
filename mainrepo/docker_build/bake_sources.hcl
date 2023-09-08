# Targets with scoped sources

target "dl_src_lib" {
  contexts = {
    lib = "${DL_B_PROJECT_ROOT}/lib"
  }
  dockerfile-inline = dl_dockerfile_prepare_src([
    { cmd = "copy", ctx = "lib", target_path = "lib" },
  ])
}
