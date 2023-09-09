#!/usr/bin/env bash

set -eux

# Expected env vars from gha
# ROOT_DIR
# REPO_OWNER
# REPO_NAME


echo "==$ROOT_DIR=="
BASE_IMG="datalens_base_ci:$(bash $ROOT_DIR/ci/get_base_img_hash.sh)"

set +x

export CR_BASE="ghcr.io/$REPO_OWNER/$REPO_NAME"
export CR_TAG_BASE_OS="$CR_BASE/dl_base_os:latest"

export CR_BASE_IMG="$CR_BASE/$BASE_IMG"
echo "Going to build and push $CR_BASE_IMG"

if ! docker image inspect $CR_BASE_IMG --format="ignore" ; then
    if ! docker pull $CR_BASE_IMG; then
      ./docker_build/run-project-bake base_ci  --set "base_ci.tags=$CR_BASE_IMG"  # --push  # saving space )
    else
      echo "Got image from registry"
    fi
else
  echo "Image available locally"
fi


CI_IMG="datalens_ci_with_code:$GIT_SHA"
# CI_MYPY_IMG="datalens_ci_mypy:$GIT_SHA"

export CR_CI_IMG_WITH_SRC="$CR_BASE/$CI_IMG"
# export CI_IMG_MYPY="$CR_BASE/$CI_MYPY_IMG"

./docker_build/run-project-bake ci_with_src  --set "ci_with_src.tags=$CR_CI_IMG_WITH_SRC" --push

## Building images with bake
#export BASE_CI_TAG_OVERRIDE=$BASE_IMG_CR
## bash docker_build/run-project-bake ci_with_src ci_mypy
#docker push $CI_IMG_MYPY
#docker push $CI_IMG_WITH_SRC
