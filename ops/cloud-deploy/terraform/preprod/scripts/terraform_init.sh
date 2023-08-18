#!/usr/bin/env bash

set -Eeuo pipefail

SECRET_ID=sec-01dc24ppx88wd8wjawzp3zwxcg

SECRET_VERSION="$(
    yav get secret -j ${SECRET_ID} \
    | jq -r '.secret_versions | max_by(.created_at) | .version'
)"

SVC_ACCT_ACCESS_KEY_ID="$(yav get version -o TF_SVC_ACCT_KEY_ID "${SECRET_VERSION}")"
SVC_ACCT_ACCESS_KEY="$(yav get version -o TF_SVC_ACCT_ACCESS_KEY "${SECRET_VERSION}")"

exec terraform init \
    "$@" \
    -backend-config="secret_key=${SVC_ACCT_ACCESS_KEY}" \
    -backend-config="access_key=${SVC_ACCT_ACCESS_KEY_ID}"
