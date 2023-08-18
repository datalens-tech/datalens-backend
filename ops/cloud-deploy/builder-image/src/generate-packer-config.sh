#!/usr/bin/env bash
set -e

source ./common.sh

echo "Path to the settings file '${1}'";

SETTINGS=$1
IMAGE_PREFIX=$2

BUILD_ENVIRONMENT="$(get_yaml_value $SETTINGS BUILD_ENVIRONMENT)"
FOLDER_ID="$(get_yaml_value $SETTINGS FOLDER_ID)"
SUBNET_ID="$(get_yaml_value $SETTINGS SUBNET_ID)"
ZONE="$(get_yaml_value $SETTINGS ZONE)"
if [ "$BUILD_ENVIRONMENT" == "preprod" ]; then
    YC_ENDPOINT="api.cloud-preprod.yandex.net:443"
    SOURCE_IMAGE_ID="fdvvot5sbrofk1vl63i4"
    SA_KEY_FILE_VARNAME="PREPROD_YC_SERVICE_ACCOUNT_KEY_FILE"
else
    BUILD_ENVIRONMENT="prod"
    YC_ENDPOINT="api.cloud.yandex.net:443"
    SOURCE_IMAGE_ID="fd8li72i73b3b8nu32s7"
    SA_KEY_FILE_VARNAME="PROD_YC_SERVICE_ACCOUNT_KEY_FILE"
fi
if ! test $FOLDER_ID; then
    throw_exception "FOLDER_ID should be specified in settings.yaml"
fi
if ! test $SUBNET_ID; then
    throw_exception "SUBNET_ID should be specified in settings.yaml"
fi
if ! test $ZONE; then
    throw_exception "ZONE should be specified in settings.yaml"
fi
echo -e "$GREEN settings.yaml is valid. $NC"

requiredEnv=("SSH_PRIVATE_KEY_FILE" $SA_KEY_FILE_VARNAME)
check_env $requiredEnv

cat ./packer-builder.json | jq \
".endpoint=\"$YC_ENDPOINT\"" | jq \
".folder_id=\"$FOLDER_ID\"" | jq \
".subnet_id=\"$SUBNET_ID\"" | jq \
".source_image_id=\"$SOURCE_IMAGE_ID\"" | jq \
".image_name=\"$IMAGE_PREFIX-`date +%s`\"" | jq \
".zone=\"$ZONE\"" | jq \
".ssh_private_key_file=\"$SSH_PRIVATE_KEY_FILE\"" | jq \
".service_account_key_file=\"${!SA_KEY_FILE_VARNAME}\""
