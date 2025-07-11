target "dl_control_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img    = "target:base_noble_db"
    bake_ctx_src_lib     = "target:dl_src_lib"
    bake_ctx_metapkg     = "target:dl_src_metapkg"
    bake_ctx_app_configs = "target:dl_app_configs"
  }
  context    = "${DL_B_PROJECT_ROOT}/app/dl_control_api"
  dockerfile = "Dockerfile"
}

target "dl_control_api_slim" {
  pull     = false
  contexts = {
    bake_ctx_base_img    = "target:base_noble_db"
    bake_ctx_src_lib     = "target:dl_src_lib"
    bake_ctx_metapkg     = "target:dl_src_metapkg"
    bake_ctx_app_configs = "target:dl_app_configs"
    bake_ctx_run         = "target:base_run"
  }
  context    = "${DL_B_PROJECT_ROOT}/app/dl_control_api"
  dockerfile = "Dockerfile.slim"
}

target "dl_data_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img    = "target:base_noble_db"
    bake_ctx_src_lib     = "target:dl_src_lib"
    bake_ctx_metapkg     = "target:dl_src_metapkg"
    bake_ctx_app_configs = "target:dl_app_configs"
  }
  context    = "${DL_B_PROJECT_ROOT}/app/dl_data_api"
  dockerfile = "Dockerfile"
}

target "dl_data_api_slim" {
  pull     = false
  contexts = {
    bake_ctx_base_img    = "target:base_noble_db"
    bake_ctx_src_lib     = "target:dl_src_lib"
    bake_ctx_metapkg     = "target:dl_src_metapkg"
    bake_ctx_app_configs = "target:dl_app_configs"
    bake_ctx_run         = "target:base_run"
  }
  context    = "${DL_B_PROJECT_ROOT}/app/dl_data_api"
  dockerfile = "Dockerfile.slim"
}
