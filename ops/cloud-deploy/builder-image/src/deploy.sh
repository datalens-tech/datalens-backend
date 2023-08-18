#!/usr/bin/env bash
set -eo pipefail

srcDir=$(dirname "$0")
source "$srcDir/common.sh"

IG_ID=$1
IMAGE_ID=$2
DEK=$3
CONFIG_DIR=$4

echo "Starting to update instance group '${IG_ID}'. New image id is '${IMAGE_ID}'";
echo "Using config directory $CONFIG_DIR"

IG_CONFIG="$CONFIG_DIR/ig-config.yaml"
if ! test -f "$IG_CONFIG"; then
    throw_exception "$IG_CONFIG file was not found"
fi

echo "Create temporary yaml file..."
ID_CONFIG_COMPILED="./tmp-ig-spec.yaml"

sed "s/__DEK__/$DEK/g" $IG_CONFIG | sed "s/__IMAGE_ID__/$IMAGE_ID/g" > "$ID_CONFIG_COMPILED"

echo "Updating instance-group with yc..."
yc compute instance-group update "$IG_ID" --file "$ID_CONFIG_COMPILED"

echo -e "$GREEN Instance group have been succesfully updated! $NC"

echo "Cleanup"
rm "$ID_CONFIG_COMPILED"
