#!/usr/bin/env bash

set -eu

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
export PROJECT_ROOT=$(realpath "${HERE}/../..")

GIT_CURRENT_BRANCH=${GIT_CURRENT_BRANCH}
PR_URL_FILE_LOCATION=${PR_URL_FILE_LOCATION}
GITHUB_STEP_SUMMARY=${GITHUB_STEP_SUMMARY}

_GIT_CURRENT_BRANCH_FROM_GIT="$(git rev-parse --abbrev-ref HEAD)"

if [[ "${GIT_CURRENT_BRANCH}" != "${_GIT_CURRENT_BRANCH_FROM_GIT}" ]]; then
  echo "Requested & actual branch doesn't match: ${GIT_CURRENT_BRANCH} != ${_GIT_CURRENT_BRANCH_FROM_GIT}"
  exit 255
fi

NEW_BRANCH_NAME="release-pr/$(date +"%Y-%m-%dT%H-%M-%S")"
git checkout -b "${NEW_BRANCH_NAME}"

bump2version minor "${PROJECT_ROOT}/.bumpversion.cfg"

NEW_VERSION=$(bump2version --list --allow-dirty --dry-run "${PROJECT_ROOT}/.bumpversion.cfg" | grep current_version | cut -f 2 -d =)
COMMIT_MSG="releasing version ${NEW_VERSION}"

git add "${PROJECT_ROOT}"
git commit -m "${COMMIT_MSG}"
git push --set-upstream origin "${NEW_BRANCH_NAME}"

# Creating PR
PR_URL=$(gh pr create --fill --base "${GIT_CURRENT_BRANCH}" --body "Releasing version ${NEW_VERSION}")
echo "${PR_URL}" >> "${GITHUB_STEP_SUMMARY}"
echo "${PR_URL}"
echo "${PR_URL}" > "${PR_URL_FILE_LOCATION}"
