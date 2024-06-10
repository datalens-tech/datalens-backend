target "dl_base_linux_w_db_bin_dependencies" {
  context = "${DL_B_PROJECT_ROOT}/docker_build/target_dl_base_linux_w_db_bin_dependencies"
  args    = {
    BASE_IMG = "${BASE_LINUX}"
  }
}

target "base_jammy_db" {
  context  = "${DL_B_PROJECT_ROOT}/docker_build/target_base_jammy_db"
  contexts = {
    bake_ctx_base_img = "target:base_jammy"
  }
}

target "base_jammy" {
  context = "${DL_B_PROJECT_ROOT}/docker_build/target_base_jammy"
}
