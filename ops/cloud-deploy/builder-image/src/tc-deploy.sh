#!/bin/sh
set -eo pipefail

export CLOUD_IMAGE_ID=$1;
export BUILD_VERSION=$2;

srcDir=$(dirname "$0")
source "$srcDir/common.sh"

DEPLOY_SETTINGS="$CONFIG_DIR/deploy-settings.yaml"
if ! test -f "$DEPLOY_SETTINGS"; then
    throw_exception "$DEPLOY_SETTINGS file was not found"
fi
FOLDER_ID="$(cat $DEPLOY_SETTINGS | yq -r .FOLDER_ID)"
CLOUD_ID="$(cat $DEPLOY_SETTINGS | yq -r .CLOUD_ID)"
INSTANCE_GROUP_ID="$(cat $DEPLOY_SETTINGS | yq -r .INSTANCE_GROUP_ID)"
BUILD_ENVIRONMENT="$(cat $DEPLOY_SETTINGS | yq -r .BUILD_ENVIRONMENT)"

requiredEnv=("FOLDER_ID" "CLOUD_ID" "INSTANCE_GROUP_ID" "BUILD_ENVIRONMENT")
check_env $requiredEnv
echo -e "$GREEN Deploy settings is valid. $NC"

echo "Cloud image id is '$CLOUD_IMAGE_ID'"

create_yc_profile

/bin/bash "$srcDir/mission-control.sh" ALLOCATING
/bin/bash "$srcDir/deploy.sh" "$INSTANCE_GROUP_ID" "$CLOUD_IMAGE_ID" "$DEK" "$CONFIG_DIR"
/bin/bash "$srcDir/mission-control.sh" DEPLOYED
