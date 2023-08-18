#!/bin/sh
set -eo pipefail

export BUILD_IMAGE_URL=$1
echo "Build image url is '$BUILD_IMAGE_URL'"

TMP="$(pwd)/tmps"
rm -rf "$TMP"
mkdir "$TMP"

NEW_KEY_FILE="$TMP/key.json"
echo "$YC_SERVICE_ACCOUNT_KEY_FILE" | jq ".private_key=\"$YC_SERVICE_ACCOUNT_PRIVATE_KEY\"" > $NEW_KEY_FILE
export YC_SERVICE_ACCOUNT_KEY_FILE=$NEW_KEY_FILE

NEW_KEY_FILE="$TMP/id_rsa"
echo "$SSH_PRIVATE_KEY_FILE" > $NEW_KEY_FILE
export SSH_PRIVATE_KEY_FILE=$NEW_KEY_FILE

srcDir=$(dirname "$0")
echo "Build all path: ${srcDir}/build.sh"

/bin/bash ${srcDir}/build.sh "$CONFIG_DIR" /tmp/manifest/
BUILD_RESULT=$?

rm -rf "$TMP"

echo "Build result is $BUILD_RESULT"
exit $BUILD_RESULT