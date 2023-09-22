#!/usr/bin/env bash

set -euxo pipefail

MAIN_REPO_COPY_DEST_DIR="${MAIN_REPO_COPY_DEST_DIR}"  # Ensure that destination dir env var set

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT=$(realpath "${HERE}/../..")
SRC="${PROJECT_ROOT}/mainrepo"

DST=$(realpath "${MAIN_REPO_COPY_DEST_DIR}")

if [[ ! -d "${DST}" ]]; then
  echo "Destination dir ${DST} is not a directory"
  exit 255
fi

cd "${SRC}"
git ls-files -m -c | rsync -avqz --files-from=- . ${DST}

# Cleanup
cd ${DST}
# TODO FIX: Realize what we need to cleanup before push
