#!/usr/bin/env bash

set -eux

echo "==$ROOT_DIR=="
BASE_IMG="datalens_base_ci:$(bash $ROOT_DIR/ci/get_base_img_hash.sh)"

set +x

export CR_BASE="ghcr.io/$REPO_OWNER/$REPO_NAME/"

export BASE_IMG_CR="$CR_BASE/$BASE_IMG"
echo "Going to build and push $BASE_IMG_CR"

echo "todo... actuall build and try pull from CR"
exit 0  # until implemented


CI_IMG="datalens_ci_with_code:$GIT_SHA"
CI_MYPY_IMG="datalens_ci_mypy:$GIT_SHA"

export CI_IMG_WITH_SRC="$CR_BASE/$CI_IMG"
export CI_IMG_MYPY="$CR_BASE/$CI_MYPY_IMG"

# Building images with bake
export BASE_CI_TAG_OVERRIDE=$BASE_IMG_CR
bash docker_build/run-project-bake ci_with_src ci_mypy
docker push $CI_IMG_MYPY
docker push $CI_IMG_WITH_SRC
