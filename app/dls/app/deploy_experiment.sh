#!/bin/sh -x

set -Eeu

cd ..
ver="$(sed -rn '/^[0-9]+\.[0-9a-z.]+$/ { p; q; }' changelog.md)tier0"
make build-tier0-suffixed
releaser push --version="$ver"  # just to re-check the version match

deploy() {
    prj_name="$1"
    env_name="$2"
    releaser deploy \
        --version="$ver" \
        --project="$prj_name" \
        --environment "$env_name" \
        --deploy-comment-format 'dls tier0 experimental rollout'
}
deploy stat beta
deploy datalens testing
../tools/build-cloud-preprod "$ver"
