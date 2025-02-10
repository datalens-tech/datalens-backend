#!/usr/bin/env bash

set -eux

# Expected env vars from gha
ROOT_DIR=${ROOT_DIR}
REPO_OWNER=${REPO_OWNER}
REPO_NAME=${REPO_NAME}
# Optional
DKR_IMG_REPO_PREFIX=${DKR_IMG_REPO_PREFIX:-"ghcr.io/${REPO_OWNER}/${REPO_NAME}"}

# Internal constants
BAKE_TARGET_BASE_CI="base_ci"
BAKE_TARGET_CI_W_SRC="ci_with_src"
BAKE_EXECUTABLE="${ROOT_DIR}/docker_build/run-project-bake"

# Will be used as base image for ci_with_src
#  Caching is used to speed-up 3rd party dependencies installation ()
#  Should be removed after implementation of bake cache
DKR_IMG_CACHED_TARGET_DL_CI_BASE="${DKR_IMG_REPO_PREFIX}/datalens_base_ci:$(bash ${ROOT_DIR}/ci/get_base_img_hash.sh)"

# CI image with all sources will be pushed here
DKR_IMG_TARGET_DL_CI_WITH_SRC="${DKR_IMG_REPO_PREFIX}/datalens_ci_with_code:${GIT_SHA}"

set +x

echo "Looking for Cached CI base image: ${DKR_IMG_CACHED_TARGET_DL_CI_BASE}"

if ! docker image inspect "${DKR_IMG_CACHED_TARGET_DL_CI_BASE}" --format="ignore" ; then
    if ! docker pull -q "${DKR_IMG_CACHED_TARGET_DL_CI_BASE}"; then
      echo "Cached CI base image not found in registry or locally. Going to build it..."
      "${BAKE_EXECUTABLE}" "${BAKE_TARGET_BASE_CI}" \
        --set "${BAKE_TARGET_BASE_CI}.tags=${DKR_IMG_CACHED_TARGET_DL_CI_BASE}" \
        --progress=plain \
#        --push  # saving space )
    else
      echo "Got image from registry"
    fi
else
  echo "Image available locally"
fi

echo "Building & pushing CI docker image with sources"
DL_B_EXT_CACHED_TARGET_BASE_CI="${DKR_IMG_CACHED_TARGET_DL_CI_BASE}" \
BUILDX_BAKE_ENTITLEMENTS_FS="0" \
  "${BAKE_EXECUTABLE}" "${BAKE_TARGET_CI_W_SRC}" \
  --set "${BAKE_TARGET_CI_W_SRC}.tags=${DKR_IMG_TARGET_DL_CI_WITH_SRC}" \
  --progress=plain \
  --push

## Building images with bake
#export BASE_CI_TAG_OVERRIDE=$BASE_IMG_CR
## bash docker_build/run-project-bake ci_with_src ci_mypy
#docker push $CI_IMG_MYPY
#docker push $CI_IMG_WITH_SRC
