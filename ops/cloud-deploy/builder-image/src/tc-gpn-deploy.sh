#!/bin/sh
set -eo pipefail

export IMAGE_NAME=$1;
export S3_URL=$2;

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

create_yc_profile

echo "Importing image to gpn..."
yc --insecure compute image create --source-uri="$S3_URL" --name="$IMAGE_NAME"
