#!/usr/bin/env bash
set -euo pipefail

# Script to solve "detected dubious ownership" error
# https://github.com/actions/runner-images/issues/6775

# expected env variables:
REPOSITORY_NAME=${REPOSITORY_NAME}

git config --global --add safe.directory .
git config --global --add safe.directory "/__w/${REPOSITORY_NAME}/${REPOSITORY_NAME}"
