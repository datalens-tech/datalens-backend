#!/usr/bin/env bash

set -Eeuo pipefail

error_exit() {
    printf "ERROR: %s\n" "$1"
    exit 1
}

. /venv/bin/activate

exec "$@"
