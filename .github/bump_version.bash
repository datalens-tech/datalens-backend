#!/usr/bin/env bash

set -ex pipefail

PROJ_ROOT="${PROJ_ROOT:-$(pwd)}"

ROOT_PKG_PATH="${ROOT_PKG_PATH}"
SUB_PKG_PATHS_LIST_STR="${SUB_PKG_PATHS_LIST_STR}"
IFS=';' read -ra SUB_PKG_PATHS <<< "${SUB_PKG_PATHS_LIST_STR}"

COMMIT_MSG_PREFIX="releasing version ${ROOT_PKG_PATH}"

cd "${PROJ_ROOT}/${ROOT_PKG_PATH}"
PREV_VERSION=$(head -n1 changelog.md)
PREV_VER_COMMIT_INFO_ARR=($(git log --pretty=reference . | grep -i "^[a-z0-9]\{6,\} (${COMMIT_MSG_PREFIX} ${PREV_VERSION} (" | cut -d ' ' -f1))

echo "PREV VER COMMIT CANDIDATES"
for PREV_VER_CANDIDATES in "${PREV_VER_COMMIT_INFO_ARR[@]}"; do
	echo "${PREV_VER_CANDIDATES}"
done
echo ""

if [[ "${#PREV_VER_COMMIT_INFO_ARR[@]}" == 1 ]]; then
  PREV_VER_COMMIT="${PREV_VER_COMMIT_INFO_ARR[0]}"
elif [[ "${#PREV_VER_COMMIT_INFO_ARR[@]}" == 0 ]]; then
  PREV_VER_COMMIT=$(git rev-list --max-parents=0 HEAD)
else
  PREV_VER_COMMIT="${PREV_VER_COMMIT_INFO_ARR[0]}"
# TODO FIX: Uncomment after migration
#  echo "Found more than one commit messages with version ${PREV_VERSION} in log: ${PREV_VER_COMMIT_INFO_ARR}"
#  exit 255
fi

git tag -a ${PREV_VERSION} -m "Temp version flag ${PWD}:${PREV_VERSION}" ${PREV_VER_COMMIT}
ya tool releaser changelog --non-interactive --version-schema major.minor.hotfix --release-type minor
ya tool releaser version
git add -u -v .

LIB_VERSION=$(sed -rn '/^[0-9]+\.[0-9a-z.]+$$/ { p; q; }' changelog.md)

# Packages with same version
for SUB_PKG_PATH in "${SUB_PKG_PATHS[@]}"; do
	echo "Update version for ${SUB_PKG_PATH}"
	cd "${PROJ_ROOT}/${SUB_PKG_PATH}"
	ya tool releaser changelog --non-interactive --version $LIB_VERSION
	ya tool releaser version --version $LIB_VERSION
  git add -u -v .
done

git commit -m "${COMMIT_MSG_PREFIX} ${LIB_VERSION}"
git tag -d ${PREV_VERSION}
