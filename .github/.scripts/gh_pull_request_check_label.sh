#!/usr/bin/env bash
set -euo pipefail

# expected env variables:
GH_TOKEN=${GH_TOKEN}  # used by gh cli
OWNER=${OWNER:-"{owner}"}
REPO=${REPO:-"{repo}"}
PULL_REQUEST_NUMBER=${PULL_REQUEST_NUMBER}
LABEL=${LABEL}

if [[ -z $PULL_REQUEST_NUMBER ]]; then
    echo "DEBUG: PULL_REQUEST_NUMBER is not set, exiting with false result..." >&2
    echo "false"
    exit 0;
fi


# check if LABEL is in labels
labels=$(gh api "/repos/${OWNER}/${REPO}/pulls/${PULL_REQUEST_NUMBER}" --jq ".labels[].name")

if [[ -z $labels ]]; then
    echo "ERROR: No labels found for PR($PULL_REQUEST_NUMBER), exiting..." >&2
    echo "false"
    exit 0;
fi

echo "DEBUG: Found labels for PR($PULL_REQUEST_NUMBER): $labels" >&2

# if label not in labels, without partial match
if [[ ! $labels =~ (^|[[:space:]])"$LABEL"($|[[:space:]]) ]]; then
    echo "ERROR: Label $LABEL not found for PR($PULL_REQUEST_NUMBER), exiting..." >&2
    echo "false"
    exit 0;
fi

echo "INFO: Label $LABEL found for PR($PULL_REQUEST_NUMBER)" >&2
echo "true"
