target "dl_base_linux_w_db_bin_dependencies" {
  context = "${DL_B_PROJECT_ROOT}/docker_build/target_dl_base_linux_w_db_bin_dependencies"
  args    = {
    BASE_IMG = "${BASE_LINUX}"
  }
}

target "base_noble_db" {
  context  = "${DL_B_PROJECT_ROOT}/docker_build/target_base_noble_db"
  contexts = {
    bake_ctx_base_img = "target:base_noble"
  }
}

target "base_noble" {
  context = "${DL_B_PROJECT_ROOT}/docker_build/target_base_noble"
}
