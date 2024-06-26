# This target contains all 3rd party Python dependencies of project
#
target "base_ci" {
  context  = "target_base_ci"
  contexts = {
    bake_ctx_base_img = "target:dl_base_linux_w_db_bin_dependencies"
    metapkg           = "target:dl_src_metapkg"
  }
  args = {
    BASE_IMG = "bake_ctx_base_img"
  }
}

# This target contains all sources & all installed local packages
#
target "ci_with_src" {
  context  = _NULL_CTX
  contexts = dl_add_bake_ctx_base_img_if_override_not_defined(
    DL_B_EXT_CACHED_TARGET_BASE_CI, # Externally set override
    "base_ci", # Parent target
    {
      bake_ctx_dl_src_app = "target:dl_src_app"
      bake_ctx_dl_src_lib = "target:dl_src_lib",
      bake_ctx_dl_src_terrarium  = "target:dl_src_terrarium"
      bake_ctx_dl_src_ci = "target:dl_src_ci"
    }
  )
  args = {
    BASE_IMG = dl_coalesce_to_bake_ctx_base_img(DL_B_EXT_CACHED_TARGET_BASE_CI)
  }
  dockerfile-inline = join("\n", [
    "ARG BASE_IMG",
    "FROM $BASE_IMG",
    "COPY --from=bake_ctx_dl_src_app / /src/",
    "COPY --from=bake_ctx_dl_src_lib / /src/",
    "COPY --from=bake_ctx_dl_src_terrarium / /src/",
    "COPY --from=bake_ctx_dl_src_ci / /src/",
    "RUN . /venv/bin/activate && pip install -e /src/terrarium/bi_ci",
    "RUN . /venv/bin/activate && pip install -e /src/terrarium/dl_gitmanager",
    "RUN . /venv/bin/activate && python -c 'import pyproject_hooks; print(pyproject_hooks.__version__)' && poetry --version",
    "RUN . /venv/bin/activate && cd /src/metapkg/ && poetry install --no-root --with=dev --with=ci",
    # todo: include stubs into the metapkg pyproject.toml
    "RUN . /venv/bin/activate && pip install -r /src/ci/requirements_types.txt"
  ])
}
