#!/usr/bin/env bash

set -eu

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
export PROJECT_ROOT=$(realpath "${HERE}/../..")

AUTO_BAKE_TARGETS_SPACE_SEPARATED="${AUTO_BAKE_TARGETS_SPACE_SEPARATED}"
GIT_CURRENT_BRANCH=${GIT_CURRENT_BRANCH}
GITHUB_STEP_SUMMARY=${GITHUB_STEP_SUMMARY}

GH_TOKEN_MAIN=${GH_TOKEN_MAIN}
GH_TOKEN_FOR_PR_APPROVE=${GH_TOKEN_FOR_PR_APPROVE}

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

jq -R 'split(" ")' <<< "${AUTO_BAKE_TARGETS_SPACE_SEPARATED}" | jq '. | {bake_targets: .}' > "${PROJECT_ROOT}/build_advise.json"

git add "${PROJECT_ROOT}"
git commit -m "${COMMIT_MSG}"
git push --set-upstream origin "${NEW_BRANCH_NAME}"

# Creating PR
PR_URL=$(gh pr create --fill --base "${GIT_CURRENT_BRANCH}" --body "Releasing version ${NEW_VERSION}")
# Approve PR with another robot
GH_TOKEN="${GH_TOKEN_MAIN}" gh pr review -a "${PR_URL}"
# Merge PR
GH_TOKEN="${GH_TOKEN_FOR_PR_APPROVE}" gh pr merge -s "${PR_URL}"
# Report PR
echo "${PR_URL}" >> "${GITHUB_STEP_SUMMARY}"

#
# Fetch commit SHA for merged PR
MERGED_PR_COMMIT_SHA=$(gh pr view --json mergeCommit "${PR_URL}" | jq .mergeCommit.oid -r)

git fetch
git checkout "${MERGED_PR_COMMIT_SHA}"

# Extract version from .bumpversion.cfg and cut patch
NEW_VERSION_MAJ_MIN="$(python -c "import configparser; config = configparser.ConfigParser(); config.read('.bumpversion.cfg'); print('.'.join(config.get('bumpversion', 'current_version').split('.')[:-1]))")"
RELEASE_BRANCH_NAME="release/${NEW_VERSION_MAJ_MIN}"

git checkout -b "${RELEASE_BRANCH_NAME}"
git push --set-upstream origin "${RELEASE_BRANCH_NAME}"
