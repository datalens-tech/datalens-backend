variable PROJECT_ROOT {
  default = ".."
}

variable ARC_ROOT {
  default = ".."
}

variable BASE_IMG_PHUSION {
  default = "registry.yandex.net/statinfra/base/phusion:focal-1.1.0"
}

variable BASE_IMG_UBUNTU_FOCAL {
  default = "registry.yandex.net/statinfra/base/ubuntu:focal-20230605"
}

variable CACHE_REGISTRY_PREFIX {
  default = null
}

function "interim_ext_cache_to" {
  params = []
  result = (CACHE_REGISTRY_PREFIX == null) ? [] : ["type=inline"]
}

function "interim_ext_cache_from" {
  params = [target_name]
  result = (CACHE_REGISTRY_PREFIX == null) ? [] : ["type=registry,ref=${CACHE_REGISTRY_PREFIX}/${target_name}:cache"]
}

function "interim_ext_cache_tags" {
  params = [target_name]
  result = (CACHE_REGISTRY_PREFIX == null) ? [] : ["${CACHE_REGISTRY_PREFIX}/${target_name}:cache"]
}

function "interim_ext_cache_output" {
  params = []
  result = (CACHE_REGISTRY_PREFIX == null) ? [] : ["type=registry"]
}
