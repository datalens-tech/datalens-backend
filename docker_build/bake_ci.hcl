target "all_sources" {
  contexts = {
    app     = "${PROJECT_ROOT}/app"
    lib     = "${PROJECT_ROOT}/lib"
    metapkg = "${PROJECT_ROOT}/ops/ci"
  }
  dockerfile-inline = <<EOT
FROM debian:bullseye AS build
RUN mkdir -p /src/ops
COPY --from=app / /src/app
COPY --from=lib / /src/lib
COPY --from=mainrepo/lib / /src/mainrepo/lib
COPY --from=metapkg / /src/ops/ci
FROM scratch
COPY --from=build /src /
EOT
}

target "app_pkg_info" {
  contexts = {
    all_src = "target:all_sources"
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
