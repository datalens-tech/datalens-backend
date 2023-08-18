#!/usr/bin/env bash

set -eux

# initial wait for the build

c=0

export LC_ALL=C

BASE_IMG=$(bash ops/ci/get_base_img_hash.sh)
REF="$CR_URI/$BASE_IMG"

while [ "$c" -lt 100 ]
do
  c=`expr $c + 1`
  if docker pull $REF; then
    c=1000
  else
    echo "sleep"
    sleep 10
  fi
done

docker inspect $REF

CI_IMG="datalens_ci_with_code:$GIT_SHA"
export CI_IMG_YC="$CR_URI/$CI_IMG"


# Going to project root, to copy sources
DOCKER_BUILDKIT=1  docker build -t $CI_IMG_YC --build-arg BASE_CI_IMAGE=$REF -f ops/ci/docker_image_ci_with_src/Dockerfile .
docker push "$CI_IMG_YC"
