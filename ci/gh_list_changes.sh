#!/bin/bash

set -x

BASE_SHA="${1:-main}" # If outside PR, defaulting to main
HEAD_SHA="${2:-$(git symbolic-ref --short HEAD)}"  # Default to the current branch
DIFF_FILTER="${DIFF_FILTER:-ACMRTD}"


DIVERGE_COMMIT=$(git merge-base $BASE_SHA $HEAD_SHA)
CHANGED_FILES=$(git diff --no-commit-id --name-only --diff-filter=$DIFF_FILTER $DIVERGE_COMMIT $HEAD_SHA || echo "")

echo "$CHANGED_FILES"

set +x
