#!/usr/bin/env bash

set -euxo pipefail

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT=$(realpath "${HERE}/../..")
DST=$(realpath $DIR_GH_CHECKOUT_SYNC)

ARC_COMMIT=$(arc rev-parse HEAD)
ARC_BRANCH_REF=$(arc status -b | head -n 1)
ARC_STATUS_TEXT=$(arc status)

# Initial checks
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

NEW_BRANCH_NAME="${USER}/$(date +"%Y-%m-%dT%H-%M-%S")"
#git fetch
#git checkout -b "${NEW_BRANCH_NAME}"
#git reset --hard origin/trunk

#BRANCH=$(git branch --show-current)
#if [[ "${BRANCH}" != "dev-sync-${USER}" ]]; then
#  echo "Unexpected branch set in mirror: ${BRANCH}"
#  exit 255
#fi
#git fetch
#git reset --hard origin/trunk

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
echo "SYNC ${ARC_BRANCH_REF} ${ARC_COMMIT}"
#git commit -m "SYNC ${ARC_BRANCH_REF} ${ARC_COMMIT}"
#git commit -m "${USER} sync branch ${ARC_BRANCH_REF} ${ARC_COMMIT}, update $(date)"
#git push --set-upstream origin "${NEW_BRANCH_NAME}"

#PR_URL=$(gh pr create -B trunk --fill --body "${ARC_STATUS_TEXT}")
#gh pr merge -s --admin "${PR_URL}"
