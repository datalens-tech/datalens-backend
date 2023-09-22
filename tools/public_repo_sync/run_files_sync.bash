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
  COMM_MSG="Sync ${PRIV_COMMIT}"
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
git reset --hard
git checkout "${DEST_TARGET_BRANCH}"
git fetch
git reset --hard origin/"${DEST_TARGET_BRANCH}"

# Remove all content from repo
find "${DST}" -not -path "${DST}/.git/*" -not -path "${DST}/.git" -delete

# Copy all files added to git
MAIN_REPO_COPY_DEST_DIR="${DST}" bash "${HERE}/dump_main_repo_with_cleanup.bash"

# Commit
git add .

if [[ $(git diff --stat HEAD) = '' ]]; then
    echo "No changes detected"
else
  if (( "${STRICT_MODE}" )); then
    PR_BRANCH="sync-pr/$(date +"%Y-%m-%dT%H-%M-%S")"

    git checkout -b "${PR_BRANCH}"
    git diff HEAD

    read -p 'Are you sure that YOU WANT TO CREATE PR to repo (type "YES")?: ' -r
    echo
    if [[ ${REPLY} == "YES" ]]; then
      # Set impersonated user
      git config user.name 'DataLens Team'
      git config user.email 'datalens-opensource@yandex-team.ru'

      # Commit
      git commit -m "${COMM_MSG}"

      # Reset impersonated user
      git config --unset user.name
      git config --unset user.email

      # push & create PR
      git push --set-upstream origin "${PR_BRANCH}"
      PR_URL=$(gh pr create -f)
    else
      echo "ABORTING"
      git reset --hard
      exit
    fi
  else
    git commit -m "${COMM_MSG}"
    git push
  fi
fi
