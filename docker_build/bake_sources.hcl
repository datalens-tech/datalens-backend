# Targets with scoped sources

target "dl_src_lib" {
  contexts = {
    lib = "${DL_B_SOURCES_ROOT}/lib"
  }
  dockerfile-inline = dl_dockerfile_prepare_src([
    { cmd = "copy", ctx = "lib", target_path = "lib" },
  ])
}

target "dl_src_ci" {
  contexts = {
    ci = "${DL_B_SOURCES_ROOT}/ci"
  }
  dockerfile-inline = dl_dockerfile_prepare_src([
    { cmd = "copy", ctx = "ci", target_path = "ci" },
  ])
}

target "dl_src_terrarium" {
  contexts = {
    terrarium = "${DL_B_SOURCES_ROOT}/terrarium"
  }
  dockerfile-inline = dl_dockerfile_prepare_src([
    { cmd = "copy", ctx = "terrarium", target_path = "terrarium" },
  ])
}

target "dl_src_metapkg" {
  contexts = {
    metapkg = "${DL_B_SOURCES_ROOT}/metapkg"
  }
  dockerfile-inline = dl_dockerfile_prepare_src([
    { cmd = "copy", ctx = "metapkg", target_path = "metapkg" },
  ])
}

target "dl_src_app" {
  contexts = {
    app = "${DL_B_SOURCES_ROOT}/app"
  }
  dockerfile-inline = dl_dockerfile_prepare_src([
    { cmd = "copy", ctx = "app", target_path = "app" },
  ])
}
