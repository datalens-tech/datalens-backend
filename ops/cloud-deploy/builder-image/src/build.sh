#!/usr/bin/env bash
set -eo pipefail

srcDir=$(dirname "$0")
source "$srcDir/common.sh"

START_TIME=$(date +%s)

echo "Path to the project folder '${1}'";

CONFIG_DIR=$1
MANIFEST_PATH=$2
tmpDirName="tmp"
ROOT_TMP=$CONFIG_DIR$tmpDirName

# Проверяем заданы ли базовые переменные окружения
requiredEnv=("DOCKER_USERNAME" "DOCKER_TOKEN" "BUILD_IMAGE_URL" "YAV_TOKEN" "SSH_PRIVATE_KEY_FILE")
check_env "$requiredEnv"
echo -e "$GREEN Basic environment variables is correct. $NC"

# Формируем имя диска по версии контейнера из docker-registry
IMAGE_NAME="`echo "$BUILD_IMAGE_URL" | sed -e 's/.*\///g' -e 's/[^a-zA-Z0-9]/-/g'`-`date +%s`"

envList=""
packerTemplate=`cat $srcDir/packer-template.json`


[ -d "${CONFIG_DIR}" ] || exit 1 # if not a directory, skip
dirname="$(basename "${CONFIG_DIR}")"
echo -e "$BOLD    Process directory $CONFIG_DIR $NORMAL"

#
# Проверяем есть ли необходимые конфиги
#
isValid=true
CONFIG="$CONFIG_DIR/config.yaml"
if ! test -f "$CONFIG"; then
    log_error "$CONFIG file was not found"
    isValid=false
fi

SETTINGS="$CONFIG_DIR/build-settings.yaml"
if ! test -f "$SETTINGS"; then
    log_error "$SETTINGS file was not found"
    isValid=false
fi

if [ $isValid = false ]; then
    echo "Skip $CONFIG_DIR"
    exit 1
fi

echo -e "$GREEN All config files are in the right place. $NC"
#
# Читаем из конфигов переменные
#
BUILD_ENVIRONMENT="$(cat $SETTINGS | yq -r .BUILD_ENVIRONMENT)"
#BUCKET="$(cat $SETTINGS | yq -r .BUCKET)"
FOLDER_ID="$(cat $SETTINGS | yq -r .FOLDER_ID)"
SUBNET_ID="$(cat $SETTINGS | yq -r .SUBNET_ID)"
ZONE="$(cat $SETTINGS | yq -r .ZONE)"
DISABLE_USING_OLD_DNS="$(cat $SETTINGS | yq -r .DISABLE_USING_OLD_DNS)"

YC_ENDPOINT="api.cloud.yandex.net:443"
# https://bb.yandexcloud.net/projects/CLOUD/repos/paas-images/browse/paas-base-g4/CHANGELOG.md
SOURCE_IMAGE_ID="fd8nrc4kdkkv1h9tt72b"

YC_SERVICE_ACCOUNT_ID=$(cat $YC_SERVICE_ACCOUNT_KEY_FILE | jq -r .service_account_id)

#
# Проверяем все ли необходимые данные определены в конфигах
#
requiredEnv=("FOLDER_ID" "SUBNET_ID" "YC_SERVICE_ACCOUNT_KEY_FILE" "ZONE")

for t in ${requiredEnv[@]}; do
    value=${!t}
    if [ "$value" == "null" ]; then
        log_error "$t is required"
        isValid=false
    fi
done
if [ $isValid = false ]; then
    echo "Skip $CONFIG_DIR"
    exit 1
fi
#check_env $requiredEnv

echo -e "$GREEN settings.yaml is valid. $NC"

tmp="$ROOT_TMP/$BUILD_ENVIRONMENT"
mkdir -p $tmp

#
# Передаем переменные в salt через Pillars
#
pillarRoot="$tmp/pillar"
mkdir -p "$pillarRoot"
cat >"$pillarRoot/top.sls" <<EOL
base:
    '*':
        - common
        - custom
EOL
    cp "$CONFIG" "$pillarRoot/custom.sls"
    cat >"$pillarRoot/common.sls" <<EOL
BUILD_IMAGE_URL: $BUILD_IMAGE_URL
BUILD_ENVIRONMENT: $BUILD_ENVIRONMENT
EOL

#
# Генерируем билдеры для packer
#
builder=$(cat $srcDir/packer-builder.json | jq \
".endpoint=\"$YC_ENDPOINT\"" | jq \
".name=\"$BUILD_ENVIRONMENT\"" | jq \
".folder_id=\"$FOLDER_ID\"" | jq \
".subnet_id=\"$SUBNET_ID\"" | jq \
".source_image_id=\"$SOURCE_IMAGE_ID\"" | jq \
".image_name=\"$IMAGE_NAME\"" | jq \
".zone=\"$ZONE\"" | jq \
".ssh_private_key_file=\"$SSH_PRIVATE_KEY_FILE\"" | jq \
".service_account_key_file=\"$YC_SERVICE_ACCOUNT_KEY_FILE\"" | jq \
".service_account_id=\"$YC_SERVICE_ACCOUNT_ID\"")

if [ -z "${SSH_BASTION_USERNAME}" ]; then
  log_error "Bastion is not using"
else
	builder_tmp=$(echo $builder | jq \
	".ssh_bastion_host=\"bastion.cloud.yandex.net\"" | jq \
	".ssh_bastion_username=\"$SSH_BASTION_USERNAME\"")
	builder=$builder_tmp;
fi

packerTemplate=$(echo "$packerTemplate" | jq ".builders[.builders| length] |= . + $builder")
envList="$envList $BUILD_ENVIRONMENT"

if [ $envList == "" ]; then
    log_error "No valid configs have been found"
    exit 1;
fi;

# Add certificate
if test -f "$CONFIG_DIR/custom.crt"; then
    # install custom cert
    installCert="{
        \"type\": \"shell\",
        \"inline\": [
            \"mkdir -p /usr/local/share/ca-certificates/\",
            \"sudo cp /tmp/custom.crt /usr/local/share/ca-certificates/\",
            \"sudo update-ca-certificates\"
        ]
    }"
    packerTemplate=$(echo "$packerTemplate" | jq ".[\"provisioners\"] |= [ $installCert ] + .")

    copyCert="{
        \"type\": \"file\",
        \"source\": \"$CONFIG_DIR/custom.crt\",
        \"destination\": \"/tmp/\"
    }"
    packerTemplate=$(echo "$packerTemplate" | jq ".[\"provisioners\"] |= [ $copyCert ] + .")
else
    # install yandex cert
    installCert="{
        \"type\": \"shell\",
        \"inline\": [
            \"mkdir -p /usr/local/share/ca-certificates/\",
            \"sudo wget https://crls.yandex.net/YandexInternalRootCA.crt -O /usr/local/share/ca-certificates/YandexInternalRootCA.crt\",
            \"sudo update-ca-certificates\"
        ]
    }"
    packerTemplate=$(echo "$packerTemplate" | jq ".[\"provisioners\"] |= [ $installCert ] + .")
fi

# Yandex Export (export image to s3)
S3_PATH=""
if [ "$BUCKET" == "null" ]; then
    echo -e "$GREEN yandex-export will not be used $NC"
else
    echo -e "$GREEN yandex-export post-processor have been added (bucket $BUCKET) $NC"
    S3_PATH="s3://$BUCKET/$IMAGE_NAME"
    yandexExport=$(cat $srcDir/yandex-export-template.json | jq \
".folder_id=\"$FOLDER_ID\"" | jq \
".subnet_id=\"$SUBNET_ID\"" | jq \
".zone=\"$ZONE\"" | jq \
".service_account_id=\"$YC_SERVICE_ACCOUNT_ID\"" | jq \
".service_account_key_file=\"$YC_SERVICE_ACCOUNT_KEY_FILE\"" | jq \
".paths[.paths| length] |= . + \"$S3_PATH\"")
    packerTemplate=$(echo "$packerTemplate" | jq ".[\"post-processors\"][.[\"post-processors\"]| length] |= . + $yandexExport")
fi

# Using old DNS if need
if [ "$DISABLE_USING_OLD_DNS" == "true" ]; then
    echo -e "$GREEN using DNS from base image $NC"
else
    echo -e "$GREEN using old DNS 2a02:6b8:0:3400::5005$NC"
    olddnsProvisioner=$(cat $srcDir/olddns-provisioner.json)
    packerTemplate=$(echo "$packerTemplate" | jq ".[\"provisioners\"][.[\"provisioners\"]| length] |= . + $olddnsProvisioner")
fi

if [ "$UPGRADE_DEB_PACKAGES" == "true" ]; then
    upgradeDebProvisioner=$(cat $srcDir/upgrade-deb-provisioner.json)
    packerTemplate=$(echo "$packerTemplate" | jq ".[\"provisioners\"][.[\"provisioners\"]| length] |= . + $upgradeDebProvisioner")
fi

echo "$packerTemplate" > "$ROOT_TMP/packer.json"
created="$(date '+%Y-%m-%d_%H:%M:%S')"

cp -r "$srcDir/vm_scripts" "$ROOT_TMP"

PACKER_RUN="packer build \
-var settings_dir=$ROOT_TMP \
-var manifest_path=$MANIFEST_PATH \
-var salt_local_root=$srcDir/salt \
-var created_time=$created \
-var s3_path=$S3_PATH \
$ROOT_TMP/packer.json"
echo "$PACKER_RUN"

set +e
($PACKER_RUN)
PACKER_RESULT=$?
echo "Packer exit with code $PACKER_RESULT"

rm -rf $TMP_DIR

IMAGE_URL=$(/usr/local/bin/aws s3 --region ru-central1 --endpoint-url=https://storage.yandexcloud.net presign ${S3_PATH} --expires-in 10800)
echo "S3 presigned image url: $IMAGE_URL"

echo "##teamcity[setParameter name='env.S3_PATH' value='$S3_PATH']"
echo "##teamcity[setParameter name='env.S3_URL_PRESIGNED' value='$IMAGE_URL']"

if [ $? -eq 1 ]; then
    exit 1
fi

END_TIME=$(date +%s)
(( "DIFF=$END_TIME-$START_TIME" ))
HUMAN_READABLE_DIFF="$(displaytime $DIFF)"
echo "Total execution time takes $HUMAN_READABLE_DIFF"

exit $PACKER_RESULT
