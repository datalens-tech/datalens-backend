#!/usr/bin/env bash
set -euo pipefail

# expected env variables:
GH_TOKEN=${GH_TOKEN}  # used by gh cli
OWNER=${OWNER:-"{owner}"}
REPO=${REPO:-"{repo}"}
USER=${USER}
PERMISSION=${PERMISSION:-"admin"}  # Possible values: admin, write, read, none

declare -A permission_index
permission_index=(["none"]=0 ["read"]=1 ["write"]=2 ["admin"]=3)

# check if PERMISSION is in permission_index
if [[ ! ${permission_index[$PERMISSION]+_} ]]; then
    echo "ERROR: PERMISSION must be one of ${!permission_index[*]}, found $PERMISSION, exiting..." >&2
    echo "false"
    exit 0;
fi


user_permission=$(gh api "/repos/${OWNER}/${REPO}/collaborators/${USER}/permission" --jq ".role_name")

if [[ -z $user_permission ]]; then
    echo "ERROR: User $USER not found in $OWNER/$REPO, exiting..." >&2
    echo "false"
    exit 0;
fi


if [[ ${permission_index[$PERMISSION]} -gt ${permission_index[$user_permission]} ]]; then
    echo "ERROR: User $USER has $user_permission permission, but $PERMISSION permission is required, exiting..." >&2
    echo "false"
    exit 0;
fi

echo "INFO: User $USER has $user_permission permission in $OWNER/$REPO, required permission is $PERMISSION" >&2
echo "true"
