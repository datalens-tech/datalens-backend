#!/usr/bin/env bash

set -euxo pipefail

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT=$(realpath "${HERE}/../..")
DST=$(realpath $DIR_GH_CHECKOUT_PROD)

ARC_COMMIT=$(arc rev-parse HEAD)
ARC_BRANCH_REF=$(arc status -b | head -n 1)
ARC_STATUS_TEXT=$(arc status)

# Initial checks
if [[ "${ARC_BRANCH_REF}" != "On branch trunk" ]]; then
    echo "Not on trunk!"
    exit 255
fi

if [[ $(arc diff --stat HEAD) != '' ]]; then
    echo "Arc repo is dirty!"
    exit 255
fi

if [[ ! -d "${DST}" ]]; then
    echo "${DST} is not exists!"
    exit 255
fi

if [[ ! -d "${DST}/.git" ]]; then
    echo "${DST} is not a git repo!"
    exit 255
fi

# Preparing target repo
cd "${DST}"

git checkout trunk
git fetch
git reset --hard origin/trunk

# Remove all content from repo
find "${DST}" -not -path "${DST}/.git/*" -not -path "${DST}/.git" -delete

# Copy all files added to arc
cd "${PROJECT_ROOT}"
arc ls-files -m -c | rsync -avz --files-from=- . ${DST}

# Cleanup
cd ${DST}
find "${DST}" -path "*tests/mypy/*" -delete
find "${DST}" -name "ya.make" -delete
find "${DST}" -name "a.yaml" -delete
find "${DST}" -name ".release.hjson" -exec bash -c "cat {} | grep -v './ya.make' > {}.tmp; rm {}; mv {}.tmp {};" \;
# arcadia entry-points (app/*/app dir)

# Commit
git add .
COMM_MSG="SYNC ${ARC_BRANCH_REF} ${ARC_COMMIT}"

if [[ $(git diff --stat HEAD) != '' ]]; then
  git commit -m "${COMM_MSG}"
  git push
else
  echo 'No changes detected!'
fi
