#!/usr/bin/env bash
set -e

case $YC_PROFILE in

    "yc-preprod")
    REGISTRY_HOST=cr.cloud-preprod.yandex.net
    REGISTRY_ID=crta0bkj59h30lbm28vu
    ;;

    "yc-prod")
    REGISTRY_HOST=cr.yandex
    REGISTRY_ID=UNKNOWN
    ;;

    "israel")
    REGISTRY_HOST=cr.il.nebius.cloud
    REGISTRY_ID=crlfgqo1pse5j9udsk53
    ;;

    "nemax")
    REGISTRY_HOST=cr.nemax.nebius.cloud
    REGISTRY_ID=crnct6nddg9f0oicma93
    ;;

esac

echo `yc iam create-token --profile yc-prod` | docker login --username iam --password-stdin cr.yandex
echo `yc iam create-token --profile $YC_PROFILE` | docker login --username iam --password-stdin $REGISTRY_HOST

docker pull cr.yandex/crpvqg9oj0ude1mj1mj2/${1}:${2}
docker tag cr.yandex/crpvqg9oj0ude1mj1mj2/${1}:${2} $REGISTRY_HOST/$REGISTRY_ID/${1}:${2}
docker push $REGISTRY_HOST/$REGISTRY_ID/${1}:${2}
