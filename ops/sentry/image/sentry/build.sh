#!/bin/bash

BUILD_VERSION=$1
SENTRY_VERSION=9.1.1

IMAGE="registry.yandex.net/statinfra/sentry"
IMAGE_VERSION="$SENTRY_VERSION-yandex.$BUILD_VERSION"

# Maybe: `--network=host`
docker build \
    --pull \
    --force-rm \
    --build-arg SENTRY_VERSION=$SENTRY_VERSION \
    -t "$IMAGE:$IMAGE_VERSION" \
    .
docker push "$IMAGE:$IMAGE_VERSION"

# docker tag "$IMAGE:$IMAGE_VERSION" "$IMAGE:latest"
# docker push "$IMAGE:latest"
