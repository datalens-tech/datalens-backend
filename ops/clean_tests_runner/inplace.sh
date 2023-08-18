#!/bin/bash
# #######
# Wrapper over `./main.sh` that only requires `YAV_TOKEN` secret.
#
# TODO: move this as a pathway into `./main.sh`
# #######

set -Eeuo pipefail

set -x

yav-deploy --debug -c ./.secrets/ -d ./

set +a
set +x  # do not echo the secrets
. ./.env
set -x
set -a

export REPO_URL REPO_BRANCH CMD EXTRADBG DOCKER_USERNAME

if [ ! -z "${FORCE_PREPARE:-}" ]; then
    ./prepare.sh
fi
if [ -z "${REPO_URLS:-}" ]; then
    REPO_URLS="${REPO_URL:-ssh://git@bb.yandex-team.ru/statbox/bi-api.git}"
else
    if [ ! -z "${REPO_URL:-}" ]; then
        echo "Do not combine REPO_URL and REPO_URLS." >&2
        exit 1
    fi
fi

set +e
for REPO_URL in $REPO_URLS; do

printf "\n\n\n ======= ======= ======= REPO_URL='%s' ======= ======= =======\n" "$REPO_URL"

MAIN="$(pwd)/main.sh"
cd "$(mktemp -d)"
export REPO_URL
"$MAIN"

done
