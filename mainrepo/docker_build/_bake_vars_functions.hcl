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

variable CR_TAG_BASE_OS {
  default = ""
}

variable CR_BASE_IMG {
  default = ""
}