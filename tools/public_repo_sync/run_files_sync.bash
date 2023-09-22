#!/usr/bin/env bash

set -euxo pipefail

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT=$(realpath "${HERE}/../..")
DST=$(realpath $PUB_SYNC_TARGET_REPO_DIR)
SRC="${PROJECT_ROOT}/mainrepo"

STRICT_MODE=${STRICT_MODE:-1}
TARGET_BRANCH=${TARGET_BRANCH:-trunk}

DEST_TARGET_BRANCH=${DEST_TARGET_BRANCH:-trunk}

PRIV_COMMIT=$(git rev-parse HEAD)
PRIV_BRANCH_REF=$(git rev-parse --abbrev-ref HEAD)

cd "${SRC}"

# Check branch & cleanness of repo
if (( "${STRICT_MODE}" )); then
  if [[ "${PRIV_BRANCH_REF}" != "trunk" ]]; then
      echo "Not on trunk!"
      exit 255
  fi
  if [[ $(git diff --stat HEAD) != '' ]]; then
      echo "Git repo is dirty!"
      exit 255
  fi
  COMM_MSG="Initial commit"
else
  MR_GIT_DIFF_STAT=$(git diff --stat HEAD -- .)
  COMM_MSG="Sync ${PRIV_BRANCH_REF} ${PRIV_COMMIT}"
  if [[ ! -z "${MR_GIT_DIFF_STAT}" ]]; then
      COMM_MSG="${COMM_MSG} DIRTY
      ${MR_GIT_DIFF_STAT}
      "
  fi
fi

# Common checks of target repo
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
if (( "${STRICT_MODE}" )); then
  echo "Not yet implemented"
  exit 255
else
  git reset --hard
  git checkout "${DEST_TARGET_BRANCH}"
  git fetch
  git reset --hard origin/"${DEST_TARGET_BRANCH}"
fi

# Remove all content from repo
find "${DST}" -not -path "${DST}/.git/*" -not -path "${DST}/.git" -delete

# Copy all files added to git
cd "${SRC}"
git ls-files -m -c | rsync -avqz --files-from=- . ${DST}

# Cleanup
cd ${DST}
# TODO FIX: Realize what we need to cleanup before push

# Commit
git add .

if (( "${STRICT_MODE}" )); then
  echo "Not yet implemented"
  exit 255
else
  if [[ $(git diff --stat HEAD) = '' ]]; then
      echo "No changes detected"
  else
    git commit -m "${COMM_MSG}"
    git push
  fi
fi
