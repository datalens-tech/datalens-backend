target "proto_collect_dc_public_api" {
  contexts = {
    proto_google = "https://github.com/googleapis/googleapis.git"
    proto_dc     = "${ARC_ROOT}/cloud/doublecloud/public-api"
  }
  dockerfile-inline = <<EOT
FROM debian:bullseye AS build
RUN mkdir -p /src
COPY --from=proto_google /google /src/google
COPY --from=proto_dc /doublecloud /src/doublecloud
RUN find /src -not -type d -not -name '*.proto' -delete
FROM scratch
COPY --from=build /src /
EOT
}

target "proto_collect_yc_apis" {
  contexts = {
    proto_google                 = "https://github.com/googleapis/googleapis.git"
    proto_yc_common              = "${ARC_ROOT}/cloud/bitbucket/common-api"
    proto_yc_pub_main            = "${ARC_ROOT}/cloud/bitbucket/public-api"
    proto_yc_priv_main           = "${ARC_ROOT}/cloud/bitbucket/private-api"
    proto_yc_priv_iam_as_main    = "${ARC_ROOT}/cloud/iam/accessservice/client/iam-access-service-api-proto/v1/private-api"
    proto_yc_priv_iam_proto_exts = "${ARC_ROOT}/cloud/iam/accessservice/client/cloud-proto-extensions/v1"
  }
  dockerfile-inline = <<EOT
FROM debian:bullseye AS build
RUN mkdir -p /src
RUN mkdir -p /src/yandex
COPY --from=proto_google /google /src/google
COPY --from=proto_yc_pub_main /yandex/cloud /src/yandex/cloud
COPY --from=proto_yc_common /yandex/cloud /src/yandex/cloud
COPY --from=proto_yc_priv_main /yandex/cloud /src/yandex/cloud
COPY --from=proto_yc_priv_iam_as_main /yandex/cloud/priv/accessservice /src/yandex/cloud/priv/accessservice
COPY --from=proto_yc_priv_iam_proto_exts /yandex/cloud/protoextensions /src/yandex/cloud/protoextensions
RUN find /src -not -type d -not -name '*.proto' -delete
FROM scratch
COPY --from=build /src /
EOT
}

target "proto_stubs_dc_public_api" {
  context  = "target_proto_stubs"
  contexts = {
    proto_all = "target:proto_collect_dc_public_api"
  }
  args = {
    PB_GEN_TARGET = "doublecloud/v1/*.proto doublecloud/visualization/v1/*.proto"
  }
}

target "proto_stubs_yc_apis" {
  context  = "target_proto_stubs"
  contexts = {
    proto_all = "target:proto_collect_yc_apis"
  }
  args = {
    PB_GEN_TARGET = join(" ", [
      "yandex/cloud/priv/iam/**/*.proto",
      "yandex/cloud/priv/oauth/**/*.proto",
      "yandex/cloud/priv/resourcemanager/v1/**.proto",
      "yandex/cloud/priv/accessservice/v2/**.proto",
      "yandex/cloud/iam/**/*.proto", # Is needed by `ydb`

      # TODO FIX: This is a transitive dependencies and stubs for them will not be generated without explicit enumeration
      #  https://stackoverflow.com/questions/2186525/how-to-use-to-find-files-recursively
      #  option to solve this problem: resolve all dependencies somehow & add it in target_proto_stubs/generate_proto_stubs.py
      "yandex/cloud/access/**/*.proto",
      "yandex/cloud/api/**/*.proto",
      "yandex/cloud/operation/**/*.proto",
      "yandex/cloud/*.proto",

      "yandex/cloud/priv/sensitive.proto",
      "yandex/cloud/priv/validation.proto",
      "yandex/cloud/priv/access/*.proto",
      "yandex/cloud/priv/console/v1/access_binding.proto",
      "yandex/cloud/priv/operation/*.proto",
      "yandex/cloud/priv/quota/*.proto",
      "yandex/cloud/priv/reference/reference.proto",
      "yandex/cloud/priv/servicecontrol/v1/*.proto",

      "yandex/cloud/protoextensions/*.proto",

      "yandex/cloud/priv/mdb/clickhouse/v1/**/*.proto",
      "yandex/cloud/priv/mdb/postgresql/v1/**/*.proto",
      "yandex/cloud/priv/mdb/mysql/v1/**/*.proto",
      "yandex/cloud/priv/mdb/greenplum/v1/**/*.proto",
    ])
  }
}

target "proto_stubs_dc_public_api_download" {
  inherits = ["proto_stubs_dc_public_api"]
  output   = ["type=local,dest=${PROJECT_ROOT}/lib/dc_public_api_proto_stubs"]
}

target "proto_stubs_yc_apis_download" {
  inherits = ["proto_stubs_yc_apis"]
  output   = ["type=local,dest=${PROJECT_ROOT}/lib/yc_apis_proto_stubs"]
}

target "legacy_base_ubuntu_runit" {
  context = "${PROJECT_ROOT}/ops/docker-base-images/base_ubuntu_runit"
  args    = {
    BASE_IMG = BASE_IMG_PHUSION
  }

  cache-to   = interim_ext_cache_to()
  cache-from = interim_ext_cache_from("legacy_base_ubuntu_runit")
  tags       = interim_ext_cache_tags("legacy_base_ubuntu_runit")
  output     = interim_ext_cache_output()
}

target "legacy_base_bi_base_mess" {
  context = "${PROJECT_ROOT}/ops/docker-base-images/bi_base_mess"
  args    = {
    BASE_IMG = "bake_ctx_base_img"
  }
  contexts = {
    bake_ctx_base_img = "target:legacy_base_ubuntu_runit"
  }

  cache-to   = interim_ext_cache_to()
  cache-from = interim_ext_cache_from("legacy_base_bi_base_mess")
  tags       = interim_ext_cache_tags("legacy_base_bi_base_mess")
  output     = interim_ext_cache_output()
}

target "base_tier_1" {
  context  = "target_base_tier_1"
  contexts = {
    bake_ctx_base_img    = "target:legacy_base_bi_base_mess"
    docker_image_base_ci = "${PROJECT_ROOT}/ops/ci/docker_image_base_ci"
  }

  cache-to   = interim_ext_cache_to()
  cache-from = interim_ext_cache_from("base_tier_1")
  tags       = interim_ext_cache_tags("base_tier_1")
  output     = interim_ext_cache_output()
}

target "base_focal_tier_1" {
  context = "target_base_focal_tier_1"
  args = {
    BASE_IMG = BASE_IMG_UBUNTU_FOCAL
  }
}

target "base_focal_db_tier_1" {
  context  = "target_base_focal_db_tier_1"
  contexts = {
    bake_ctx_base_img = "target:base_focal_tier_1"
  }
}

target "app_bi_api" {
  pull       = false
  contexts = {
    bake_ctx_base_img = "target:base_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app/bi_api"
  dockerfile = "Dockerfile.tier1"
}

target "app_yc_control_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img   = "target:base_focal_db_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app_yc/app_yc_control_api"
  dockerfile = "Dockerfile.tier1"
}

target "app_yc_data_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img   = "target:base_focal_db_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app_yc/app_yc_data_api"
  dockerfile = "Dockerfile.tier1"
}

target "app_yc_data_api_sec_embeds" {
  pull     = false
  contexts = {
    bake_ctx_base_img   = "target:base_focal_db_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app_yc/app_yc_data_api_sec_embeds"
  dockerfile = "Dockerfile.tier1"
}

target "app_yc_file_uploader_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img   = "target:base_focal_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app_yc/app_yc_file_uploader_api"
  dockerfile = "Dockerfile.tier1"
}

target "app_yc_file_uploader_worker" {
  pull     = false
  contexts = {
    bake_ctx_base_img   = "target:base_focal_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app_yc/app_yc_file_uploader_worker"
  dockerfile = "Dockerfile.tier1"
}

target "app_yc_public_dataset_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img   = "target:base_focal_db_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app_yc/app_yc_public_dataset_api"
  dockerfile = "Dockerfile.tier1"
}

target "app_bi_file_secure_reader" {
  pull     = false
  contexts = {
    bake_ctx_base_img = "target:base_focal_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app/bi_file_secure_reader"
  dockerfile = "Dockerfile.tier1"
}

target "app_bi_external_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img = "target:base_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app/bi_external_api"
  dockerfile = "Dockerfile.tier1"
}

target "app_bi_rqe_ext_async" {
  pull     = false
  contexts = {
    bake_ctx_base_img   = "target:base_focal_db_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app/bi_rqe_ext_async"
  dockerfile = "Dockerfile.tier1"
}

target "app_bi_rqe_ext_sync" {
  pull     = false
  contexts = {
    bake_ctx_base_img   = "target:base_focal_db_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app/bi_rqe_ext_sync"
  dockerfile = "Dockerfile.tier1"
}

target "app_bi_rqe_int_sync" {
  pull     = false
  contexts = {
    bake_ctx_base_img = "target:base_focal_db_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app/bi_rqe_int_sync"
  dockerfile = "Dockerfile.tier1"
}

target "app_bi_file_uploader" {
  pull     = false
  contexts = {
    bake_ctx_base_img = "target:base_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app/bi_file_uploader"
  dockerfile = "Dockerfile.tier1"
}

target "app_bi_file_uploader_worker" {
  pull     = false
  contexts = {
    bake_ctx_base_img = "target:base_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/app/bi_file_uploader_worker"
  dockerfile = "Dockerfile.tier1"
}

target "app_os_control_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img = "target:base_focal_db_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/mainrepo/app/dl_control_api"
  dockerfile = "Dockerfile.tier1"
}

target "app_os_data_api" {
  pull     = false
  contexts = {
    bake_ctx_base_img = "target:base_focal_db_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/mainrepo/app/dl_data_api"
  dockerfile = "Dockerfile.tier1"
}

target "integration_tests" {
  pull     = false
  contexts = {
    bake_ctx_base_img = "target:base_focal_tier_1"
    bake_ctx_src_lib  = "target:src_lib"
  }
  context    = "${PROJECT_ROOT}/ops/bi_integration_tests"
  dockerfile = "Dockerfile.tier1"
}

target "update-po" {
  pull = false
  args = {
    PACKAGE_NAME = ".." # need to pass via --set flag
    DOMAIN_NAME = ".." # need to pass via --set flag
    PATH_MASK = ".." # need to pass via --set flag
  }
  contexts = {
    bake_ctx_base_img = "target:base_tier_1"
    src               = ".." # need to pass via --set flag
  }

  dockerfile = "./target_update_po/Dockerfile"
  output     = ["type=local,dest=."] # need to pass via --set flag
}

target "msgfmt" {
  pull = false
  args = {
    PACKAGE_NAME = ".." # need to pass via --set flag
    DOMAIN_NAME = ".." # need to pass via --set flag
  }
  contexts = {
    bake_ctx_base_img = "target:base_tier_1"
    src               = ".." # need to pass via --set flag
  }

  dockerfile = "./target_msgfmt/Dockerfile"
  output     = ["type=local,dest=."] # need to pass via --set flag
}
