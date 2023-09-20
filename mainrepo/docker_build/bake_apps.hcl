target "app_os_control_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img = "target:base_focal_db"
    bake_ctx_src_lib  = "target:dl_src_lib"
    bake_ctx_metapkg  = "target:dl_src_metapkg"
  }
  context    = "${DL_B_PROJECT_ROOT}/app/dl_control_api"
  dockerfile = "Dockerfile"
}

target "app_os_data_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img = "target:base_focal_db"
    bake_ctx_src_lib  = "target:dl_src_lib"
    bake_ctx_metapkg  = "target:dl_src_metapkg"
  }
  context    = "${DL_B_PROJECT_ROOT}/app/dl_data_api"
  dockerfile = "Dockerfile"
}

target "base_focal_db" {
  context  = "target_base_focal_db"
  contexts = {
    bake_ctx_base_img = "target:base_focal"
  }
}

target "base_focal" {
  context = "target_base_focal"
}
