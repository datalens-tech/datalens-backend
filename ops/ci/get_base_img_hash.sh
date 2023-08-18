#!/usr/bin/env bash

# to be invoked from the project root (../.. from this file)

set -eu


export BASE_IMG_CTX=docker_image_base_ci

IMG_HASH_DOCKER_IMAGE_DIR="$(find ops/ci/docker_image_base_ci  -type f -print0   | sort -z | xargs -0 sha1sum -z | sha1sum  | cut -d \  -f1)"
# TODO FIX: Remove???
IMG_HASH_ANTLR="$(find lib/bi_formula/bi_formula/parser/antlr  -type f -print0   | sort -z | xargs -0 sha1sum -z | sha1sum  | cut -d \  -f1)"
IMG_HASH=$(echo "rebuild_flag:4:$IMG_HASH_DOCKER_IMAGE_DIR:$IMG_HASH_ANTLR" | sha1sum | cut -d \   -f1 )

BASE_IMG="datalens_base_ci:$IMG_HASH"

echo $BASE_IMG
