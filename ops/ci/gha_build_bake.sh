#!/usr/bin/env bash

set -x

# Set img names and export them
export LC_ALL=C
BASE_IMG="datalens_base_ci:$(bash ops/ci/get_base_img_hash.sh)"
CI_IMG="datalens_ci_with_code:$GIT_SHA"
CI_MYPY_IMG="datalens_ci_mypy:$GIT_SHA"

export BASE_CI_IMG="$CR_URI/$BASE_IMG"
export CI_IMG_WITH_SRC="$CR_URI/$CI_IMG"
export CI_IMG_MYPY="$CR_URI/$CI_MYPY_IMG"
export CI_IMG_CODE_QUALITY="$CR_URI/datalens_ci_code_quality:$GIT_SHA"

# Wait for the base image presence and pull it
c=0
while [ "$c" -lt 100 ]
do
  c=`expr $c + 1`
  if docker pull $BASE_CI_IMG; then
    c=1000
  else
    echo "sleep"
    sleep 10
  fi
done
docker inspect $BASE_CI_IMG

# Building images with bake
export BASE_CI_TAG_OVERRIDE=$BASE_CI_IMG
bash docker_build/run-project-bake ci_with_src ci_mypy ci_code_quality
docker push $CI_IMG_MYPY
docker push $CI_IMG_WITH_SRC
docker push $CI_IMG_CODE_QUALITY