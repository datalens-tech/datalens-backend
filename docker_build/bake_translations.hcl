target "base_translations" {
  context  = "target_translations/target_base"
  contexts = {
    bake_ctx_base_img = "target:base_ci"
    metapkg           = "target:dl_src_metapkg"
  }
  args = {
    BASE_IMG = "bake_ctx_base_img"
  }
}

target "update-po" {
  pull = false
  args = {
    PACKAGE_NAME       = ".." # need to pass via --set flag
    MSGID_BUGS_ADDRESS = "datalens-opensource@yandex-team.ru"
  }
  contexts = {
    bake_ctx_base_img = "target:base_translations"
    src               = ".." # need to pass via --set flag
    src_terrarium     = "target:dl_src_terrarium"
  }

  dockerfile = "./target_translations/target_update_po/Dockerfile"
  output     = ["type=local,dest=."] # need to pass via --set flag
}

target "msgfmt" {
  pull = false
  args = {
    PACKAGE_NAME = ".." # need to pass via --set flag
  }
  contexts = {
    bake_ctx_base_img = "target:base_translations"
    src               = ".." # need to pass via --set flag
    src_terrarium     = "target:dl_src_terrarium"
  }

  dockerfile = "./target_translations/target_binaries/Dockerfile"
  output     = ["type=local,dest=."] # need to pass via --set flag
}
