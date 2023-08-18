#!/usr/bin/env bash
set -euo pipefail

url="${MISSION_CONTROL_URL:-}"
deploy_env="${MISSION_CONTROL_ENV:-}"
package_name="${PACKAGE_NAME:-}"

if [[ -z ${url} ]]; then
    echo "MISSION_CONTROL_URL not set"
elif [[ -z ${deploy_env} ]]; then
    echo "MISSION_CONTROL_ENV not set"
else
    curl -H "Content-Type: application/json" \
        --request POST \
        --data "{\"environmentId\": \"${deploy_env}\", \"status\": \"${1}\", \"version\": \"${BUILD_VERSION}\", \"comment\": \"${package_name}:${BUILD_VERSION}\"}" \
        "${MISSION_CONTROL_URL}"
fi
