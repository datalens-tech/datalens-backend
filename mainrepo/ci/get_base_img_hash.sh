#!/usr/bin/env bash

# to be invoked from the project root (../.. from this file)

set -eu

export LC_ALL=C
IMG_HASH_DOCKER_IMAGE_DIR="$(find $ROOT_DIR/metapkg  -type f -print0   | sort -z | xargs -0 sha1sum -z | sha1sum  | cut -d \  -f1)"
echo "rebuild_flag:5:$IMG_HASH_DOCKER_IMAGE_DIR" | sha1sum | cut -d \  -f1
