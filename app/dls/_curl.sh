#!/bin/bash

set -Eeuo pipefail

DLS_HOST="${DLS_HOST:-http://[::1]:38080/_dls/}"
uri="$1"
# Some extra convenience:
uri="$(printf "%s" "$uri" | sed -r 's,^/_dls/,,')"
shift 1
exec curl -v \
     -H "X-API-Key: $DLS_API_KEY" \
     -H "X-User-Id: ${DLS_CURL_USER:-system_user:root}" \
     -H "X-DL-Allow-Superuser: 1" \
     -H "Content-Type: application/json" \
     "$DLS_HOST$uri" \
     "$@" | \
    fjson.py
