#!/usr/bin/env bash

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color
BOLD='\e[1;30m'
NORMAL='\e[0;30m'

function throw_exception() {
    echo -e "$RED $1 $NC"
    exit 1
}

function log_error() {
    echo -e "$RED $1 $NC"
}

function get_yaml_value() {
    db=$(grep -A3 $2: $1 | head -n1 | awk '{ print $2}')
    echo $db;
}

function get_yc_endpoint() {
    if [ "$BUILD_ENVIRONMENT" == "preprod" ]; then
        echo "api.cloud-preprod.yandex.net:443"
    elif [ "$BUILD_ENVIRONMENT" == "gpn" ]; then
        echo "api.gpn.yandexcloud.net:443"
    else
        echo "api.cloud.yandex.net:443"
    fi
}

function displaytime {
    local T=$1
    local D=$((T/60/60/24))
    local H=$((T/60/60%24))
    local M=$((T/60%60))
    local S=$((T%60))
    (( $D > 0 )) && printf '%d days ' $D
    (( $H > 0 )) && printf '%d hours ' $H
    (( $M > 0 )) && printf '%d minutes ' $M
    (( $D > 0 || $H > 0 || $M > 0 )) && printf 'and '
    printf '%d seconds\n' $S
}

function check_env {
    requiredEnv=$1
    for t in ${requiredEnv[@]}; do
        value=${!t}
        if ! test $value; then
            throw_exception "$t have to be set in environment variables"
        fi
    done
}

function create_yc_profile {
    srcDir=$(dirname "$0")
    echo "Create service account key file from environment variable"
    NEW_KEY_FILE="$srcDir/key.json"
    echo "$YC_SERVICE_ACCOUNT_KEY_FILE" | jq ".private_key=\"$YC_SERVICE_ACCOUNT_PRIVATE_KEY\"" > $NEW_KEY_FILE

    PROFILE_NAME="sa-console-prod"

    YC_ENDPOINT="$(get_yc_endpoint $BUILD_ENVIRONMENT)"
    echo "YC endpoint is $YC_ENDPOINT"

    echo "Check if profile exists..."
    set +e
    (yc config profile list | grep "$PROFILE_NAME")
    GREP_RESULT=$?
    set -e

    if [ $GREP_RESULT = 1 ]; then
        echo "Creating profile..."
        yc config profile create $PROFILE_NAME
        yc config set cloud-id $CLOUD_ID
        yc config set folder-id $FOLDER_ID
        yc config set endpoint $YC_ENDPOINT
        yc config set service-account-key $NEW_KEY_FILE
        echo "Profile $PROFILE_NAME have been created"
    else
      echo "Profile is exists, using it"
      yc config profile activate "$PROFILE_NAME"
    fi
}
