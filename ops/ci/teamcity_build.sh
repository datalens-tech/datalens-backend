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

BASE_IMG=$(bash ops/ci/get_base_img_hash.sh)

echo $BASE_IMG

export BASE_IMG_YN="registry.yandex.net/$DOCKER_REGISTRY/$BASE_IMG"
export BASE_IMG_YC="cr.yandex/$DOCKER_REGISTRY_YC_EXT/$BASE_IMG"
export BASE_IMG_YC_LATEST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/datalens_base_ci:latest"

if ! docker image inspect $BASE_IMG_YC --format="ignore" ; then
    if ! docker pull $BASE_IMG_YC; then
        echo "Going to build $BASE_IMG_YN, $BASE_IMG_YC"
        DOCKER_BUILDKIT=1 docker build -t $BASE_IMG_YN -t $BASE_IMG_YC -f ops/ci/$BASE_IMG_CTX/Dockerfile ops/ci/$BASE_IMG_CTX
        docker image tag $BASE_IMG_YC $BASE_IMG
        echo "Pushing base ci images"
        # docker push "$BASE_IMG_YN"
        docker push "$BASE_IMG_YC"
        echo "Done"
    else
        echo "Docker pull $BASE_IMG_YC done"
    fi
else
    echo "Img with third party dependencies exists, skip build";
fi

docker image tag $BASE_IMG_YC $BASE_IMG
docker image tag $BASE_IMG_YC $BASE_IMG_YC_LATEST
docker push $BASE_IMG_YC_LATEST


pull_push () {
    if ! docker manifest inspect $2 > /dev/null; then
        echo "Replicating image from $1 to $2"
        docker pull $1
        docker image tag $1 $2
        docker push $2
    else
        echo "Image $2 already pushed, skip"
    fi
}

echo "mirroring service images"

US_SRC="registry.yandex.net/data-ui/united-storage:latest"
US_DST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/united-storage:latest"
pull_push $US_SRC $US_DST

S3_SRC="registry.yandex.net/statinfra/s3cloudserver@sha256:b53e57829cf7df357323e60a19c9f98d2218f1b7ccb1d7cea5761a5a227a9ee3"
S3_DST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/s3cloudserver"
pull_push $S3_SRC $S3_DST

CH_22_SRC="registry.yandex.net/statinfra/clickhouse-server:22.11"
CH_22_DST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/clickhouse-server:22.11"
pull_push $CH_22_SRC $CH_22_DST

CH_22_3_SRC="registry.yandex.net/statinfra/clickhouse-server@sha256:74e094253baa15b18ec1ea3a79fb4015451de3bb23c40c99dcc37d1f85c1ac09"
CH_22_3_DST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/clickhouse-server:22.3"
pull_push $CH_22_3_SRC $CH_22_3_DST

MYSQL_SRC="registry.yandex.net/statinfra/mysql@sha256:d8e4032005f53a774f2361281ebf61fa3d7635d5dacf9c58bc54e823ddcf9f1d"
MYSQL_DST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/mysql:5.6"
pull_push $MYSQL_SRC $MYSQL_DST

MYSQL_8012_SRC="registry.yandex.net/statinfra/mysql@sha256:574bf8a61e3276788bcaa9a9e226977ea3037f439295e2a07b624b8aaebd66d4"
MYSQL_8012_DST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/mysql:8.0.12"
pull_push $MYSQL_8012_SRC $MYSQL_8012_DST

ORA_SRC="registry.yandex.net/statinfra/oracle-database-enterprise@sha256:25b0ec7cc3987f86b1e754fc214e7f06761c57bc11910d4be87b0d42ee12d254"
ORA_DST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/oracle-database-enterprise:12.2.0.1-slim"
pull_push $ORA_SRC $ORA_DST

INIT_DB_SRC="registry.yandex.net/statinfra/base/bi/initdb@sha256:41477e01d5e1017d31c09776ded1b135ce7e58add715a024fc294c0490b95c44"
INIT_DB_DST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/initdb:41477"
pull_push $INIT_DB_SRC $INIT_DB_DST

INIT_DB_LATEST_SRC="registry.yandex.net/statinfra/base/bi/initdb:latest"
INIT_DB_LATEST_DST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/initdb:latest"
pull_push $INIT_DB_LATEST_SRC $INIT_DB_LATEST_DST

# access denied, copy by hand
#YDB_SRC="registry.yandex.net/yandex-docker-local-ydb@sha256:882755b316b72490702e372e82c84df770b046fd3ecdd77163fc088a82c043a1"
#YDB_DST="cr.yandex/$DOCKER_REGISTRY_YC_EXT/yandex-docker-local-ydb:882755"
#pull_push $YDB_SRC $YDB_DST

echo "Finished"
rm -rf /tmp/docker_tmp


# todo: actions to cleanup ol images
