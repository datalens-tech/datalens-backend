#!/usr/bin/env bash

set -eux
export BASE_IMG_CTX=docker_image_base_ci

# need this to login into yc cr,
rm -rf /tmp/docker_tmp
mkdir -p /tmp/docker_tmp/
echo "{}\n" >> /tmp/docker_tmp/config.json
export DOCKER_CONFIG=/tmp/docker_tmp/

set +x
echo "$DOCKER_TOKEN" | docker login --username $DOCKER_USER --password-stdin registry.yandex.net
echo "going to login into cr.yandex"
echo "$DOCKER_REGISTRY_YC_EXT_KEY" | docker login --username json_key --password-stdin cr.yandex
set -x

export LC_ALL=C

BASE_IMG="datalens_base_ci:$(bash ops/ci/get_base_img_hash.sh)"

echo $BASE_IMG

export BASE_IMG_YC="cr.yandex/$DOCKER_REGISTRY_YC_EXT/$BASE_IMG"
export BASE_IMG_YC_LATEST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/datalens_base_ci:latest"

docker buildx version

if ! docker image inspect $BASE_IMG_YC --format="ignore" ; then
    if ! docker pull $BASE_IMG_YC; then
        echo "Going to build $BASE_IMG_YC"
        # ./docker_build/run-project-bake --set "base_ci_with_3rd_party_preinstalled.tags=${BASE_IMG_YC}"  base_ci_with_3rd_party_preinstalled
        ./docker_build/run-project-bake --set "base_ci.tags=${BASE_IMG_YC}"  base_ci
    else
        echo "Docker pull $BASE_IMG_YC done"
    fi
else
    echo "Img with third party dependencies exists, skip build";
fi

docker image tag $BASE_IMG_YC $BASE_IMG
docker image tag $BASE_IMG_YC $BASE_IMG_YC_LATEST
docker push $BASE_IMG_YC
docker push $BASE_IMG_YC_LATEST

bash ./ops/ci/replicate_containers.sh

rm -rf /tmp/docker_tmp
