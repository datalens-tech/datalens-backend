# let's rename this file, since it's related not only for CI

target "src_lib" {
  contexts = {
    lib     = "${PROJECT_ROOT}/lib"
    mr-lib  = "${PROJECT_ROOT}/mainrepo/lib"
    metapkg = "${PROJECT_ROOT}/ops/ci"
  }
  dockerfile-inline = <<EOT
FROM debian:bullseye AS build
RUN mkdir -p /src/ops
COPY --from=lib / /src/lib
COPY --from=mr-lib / /src/mainrepo/lib
COPY --from=metapkg / /src/ops/ci
FROM scratch
COPY --from=build /src /
EOT
}

target "src_all" {
  contexts = {
    src_lib = "target:src_lib"
    app     = "${PROJECT_ROOT}/app"
    app_yc  = "${PROJECT_ROOT}/app_yc"
    app_mr  = "${PROJECT_ROOT}/mainrepo/app"
    bi_integration_tests = "${PROJECT_ROOT}/ops/bi_integration_tests"
  }
  dockerfile-inline = <<EOT
FROM debian:bullseye AS build
COPY --from=src_lib / /src
COPY --from=app / /src/app
COPY --from=app_yc / /src/app_yc
COPY --from=app_mr / /src/mainrepo/app
COPY --from=bi_integration_tests / /src/ops/bi_integration_tests
FROM scratch
COPY --from=build /src /
EOT
}

target "src_terrarium" {
  contexts = {
    terrarium     = "${PROJECT_ROOT}/mainrepo/terrarium"
  }
  dockerfile-inline = <<EOT
FROM debian:bullseye AS build
COPY --from=terrarium / /src/terrarium
FROM scratch
COPY --from=build /src /
EOT
}

target "app_pkg_info" {
  contexts = {
    all_src = "target:src_all"
  }

  dockerfile-inline = <<EOT
FROM debian:bullseye AS build
COPY  --from=all_src / /src
RUN find /src/app ! -name pyproject.toml -type f -delete
RUN find /src/lib ! -name pyproject.toml -type f -delete
RUN find /src/ops ! -name pyproject.toml ! -name poetry.lock -type f -delete
RUN find /src -type d -empty -delete
RUN find /src
FROM scratch
COPY --from=build /src /
EOT
}

target "base_ci" {
  contexts = {
    base_img = "target:legacy_base_bi_base_mess"
  }
  context = "${PROJECT_ROOT}/ops/ci/docker_image_base_ci"
  args = {
    BASE_IMG = "base_img"
  }
}

target "ci_with_src" {
  contexts = (BASE_CI_TAG_OVERRIDE == null) ? {
    base_img = "target:base_ci"
    src_all  = "target:src_all"
    src_terrarium = "target:src_terrarium"
  } : {
    src_all  = "target:src_all"
    src_terrarium = "target:src_terrarium"
  }
  context = "${PROJECT_ROOT}/ops/ci/docker_image_ci_with_src"
  args = {
    BASE_CI_IMAGE = (BASE_CI_TAG_OVERRIDE == null) ? "base_img" : BASE_CI_TAG_OVERRIDE
  }
  tags = ["${CI_IMG_WITH_SRC}"]
}

target "ci_mypy" {
  contexts = {
    base_img = "target:ci_with_src"
  }
  tags = ["${CI_IMG_MYPY}"]
  dockerfile-inline = <<EOT
FROM base_img
RUN . /venv/bin/activate && pip install --no-deps -r /data/ops/ci/docker_image_ci_mypy/requirements_types.txt
EOT
}

target "3rd_party_requirements_txt" {
  contexts = {
    bake_ctx_all_pkg_info = "target:app_pkg_info"
  }
  dockerfile-inline = <<EOT
FROM python:3.11 AS build
RUN pip install -U pip && pip install poetry==1.5.0
COPY --from=bake_ctx_all_pkg_info / /src
WORKDIR /src/ops/ci
RUN poetry export --without-hashes --with=dev --with=ci --format=requirements.txt | grep -v "file://" > /requirements.txt

FROM scratch
COPY --from=build /requirements.txt /requirements.txt
EOT
}

target "base_ci_with_3rd_party_preinstalled" {
  cache-to   = interim_ext_cache_to()
  cache-from = interim_ext_cache_from("base_ci_with_3rd_party_preinstalled")
  tags       = interim_ext_cache_tags("base_ci_with_3rd_party_preinstalled")
  output     = interim_ext_cache_output()

  contexts = {
    bake_ctx_3rd_party_requirements_txt = "target:3rd_party_requirements_txt"
    base_img                            = "target:base_tier_1"
  }
  dockerfile-inline = <<EOT
FROM base_img
COPY --from=bake_ctx_3rd_party_requirements_txt /requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
EOT
}

target "gen-formula-ref" {
  args = {
    LOCALE_NAME = ".." # need to pass via --set flag
    OUTPUT_DIR = ".." # need to pass via --set flag
    CONFIG_VERSION = ".." # need to pass via --set flag
  }
  contexts = {
    base_img = "target:ci_with_src"
  }

  dockerfile = "./target_gen_formula_ref/Dockerfile"
  output     = ["type=local,dest=."] # need to pass via --set flag
}
