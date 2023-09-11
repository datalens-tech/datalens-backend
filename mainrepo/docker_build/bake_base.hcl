target "dl_base_linux_w_db_bin_dependencies" {
  context = "${DL_B_PROJECT_ROOT}/docker_build/target_dl_base_linux_w_db_bin_dependencies"
  args    = {
    BASE_IMG = "${BASE_LINUX}"
  }
}
