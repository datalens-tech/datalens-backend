# All variables should be prefixed with `DL_B_`

variable BASE_LINUX {
  default = "ubuntu:22.04"
}

variable DL_B_PROJECT_ROOT {
  default = ".."
}

variable DL_B_FILE_OPS_IMG {
  default = "debian:bookworm-slim"
}

variable DL_B_EXT_CACHED_TARGET_BASE_CI {
  default = ""
}

function dl_add_bake_ctx_base_img_if_override_not_defined {
  params = [base_img_override, base_target, ctx_map]
  result = merge(
    ctx_map,
    base_img_override != "" ? {} : {
      bake_ctx_base_img = "target:${base_target}"
    }
  )
}

function dl_coalesce_to_bake_ctx_base_img {
  params = [ext_base_img]
  result = ext_base_img == "" ? "bake_ctx_base_img" : ext_base_img
}

function dl_dockerfile_prepare_src {
  params = [cmd_list]
  result = join("\n", concat(
    [
      "FROM ${DL_B_FILE_OPS_IMG} AS build",
    ],
    [
    for cmd in cmd_list :
    ((cmd.cmd == "copy") ? "COPY --from=${cmd.ctx} / /src/${cmd.target_path}" : "RUN ${cmd.run}")
    ],
    [
      "FROM scratch",
      "COPY --from=build /src /",
    ]
  ))
}

# Context without any meanfull files to use with inline Dockerfile
variable _NULL_CTX {
  default = "${DL_B_PROJECT_ROOT}/docker_build/null_context"
}
