#!/bin/bash

SECRETS_FOLDER="/data/secrets"
MACHINEKEY_FOLDER="/data/machinekey"

ZITADEL_URL="zitadel:8080"
DATALENS_UI_URL="datalens-ui:8080"

ensure_jq() {
  if ! command -v jq &> /dev/null; then
    apt-get update
    apt-get install -y jq
  fi
}

ensure_curl() {
  if ! command -v curl &> /dev/null; then
    apt-get update
    apt-get install -y curl
  fi
}

set_secret() {
  SECRET_NAME=$1
  SECRET_VALUE=$2

  if [ -z "$SECRET_NAME" ]; then
    echo "Secret name is empty"
    exit 1
  fi

  if [ -z "$SECRET_VALUE" ]; then
    echo "Secret value is empty"
    exit 1
  fi

  echo "$SECRET_VALUE" > "$SECRETS_FOLDER/$SECRET_NAME"
}

wait_for_file() {
  FILE_PATH=$1

  if [ -z "$FILE_PATH" ]; then
    echo "File path is empty"
    exit 1
  fi

  while [ ! -f "$FILE_PATH" ]; do
    echo "Waiting for $FILE_PATH to be created"
    sleep 1
  done
}

wait_api() {
  INSTANCE_URL=$1
  PAT=$2

  set +e
  while true; do
    curl -s --fail -o /dev/null \
      "$ZITADEL_URL/auth/v1/users/me" \
      -H "Authorization: Bearer $PAT"
    if [[ $? -eq 0 ]]; then
      break
    fi
    printf "Waiting for Zitadel to become ready"
    sleep 1
  done
  echo " done"
  set -e
}


handle_zitadel_request_response() {
  ensure_jq

  PARSED_RESPONSE=$1
  FUNCTION_NAME=$2
  RESPONSE=$3
  if [[ $PARSED_RESPONSE == "null" ]]; then
    echo "ERROR calling $FUNCTION_NAME:" $(echo "$RESPONSE" | jq -r '.message') > /dev/stderr
    exit 1
  fi
  sleep 1
}

create_new_project() {
  INSTANCE_URL=$1
  PAT=$2
  PROJECT_NAME=$3

  RESPONSE=$(
    curl -sS -X POST "$INSTANCE_URL/management/v1/projects" \
      -H "Authorization: Bearer $PAT" \
      -H "Content-Type: application/json" \
      -d '{"name": "'"$PROJECT_NAME"'"}'
  )

  PARSED_RESPONSE=$(echo "$RESPONSE" | jq -r '.id')
  handle_zitadel_request_response "$PARSED_RESPONSE" "create_new_project_project_id" "$RESPONSE"
  echo "$PARSED_RESPONSE"
}

create_new_application() {
  INSTANCE_URL=$1
  PAT=$2
  APPLICATION_NAME=$3
  BASE_REDIRECT_URL1=$4
  LOGOUT_URL=$5
  ZITADEL_DEV_MODE=$6

  GRANT_TYPES='["OIDC_GRANT_TYPE_AUTHORIZATION_CODE","OIDC_GRANT_TYPE_REFRESH_TOKEN"]'

  RESPONSE=$(
    curl -sS -X POST "$INSTANCE_URL/management/v1/projects/$PROJECT_ID/apps/oidc" \
      -H "Authorization: Bearer $PAT" \
      -H "Content-Type: application/json" \
      -d '{
    "name": "'"$APPLICATION_NAME"'",
    "redirectUris": [
      "'"$BASE_REDIRECT_URL1"'"
    ],
    "postLogoutRedirectUris": [
       "'"$LOGOUT_URL"'"
    ],
    "RESPONSETypes": [
      "OIDC_RESPONSE_TYPE_CODE"
    ],
    "grantTypes": '"$GRANT_TYPES"',
    "appType": "OIDC_APP_TYPE_WEB",
    "authMethodType": "OIDC_AUTH_METHOD_TYPE_POST",
    "version": "OIDC_VERSION_1_0",
    "devMode": '"$ZITADEL_DEV_MODE"',
    "accessTokenType": "OIDC_TOKEN_TYPE_BEARER",
    "idTokenRoleAssertion": true,
    "idTokenUserinfoAssertion": true,
    "accessTokenRoleAssertion": true,
    "skipNativeAppSuccessPage": true
  }'
  )

  APP_ID=$(echo "$RESPONSE" | jq -r '.appId')
  handle_zitadel_request_response "$APP_ID" "create_new_application_app_id" "$RESPONSE"

  APP_CLIENT_ID=$(echo "$RESPONSE" | jq -r '.clientId')
  handle_zitadel_request_response "$APP_CLIENT_ID" "create_new_application_client_id" "$RESPONSE"

  APP_CLIENT_SECRET=$(echo "$RESPONSE" | jq -r '.clientSecret')
  handle_zitadel_request_response "$APP_CLIENT_SECRET" "create_new_application_client_secret" "$RESPONSE"
}

create_service_user() {
  INSTANCE_URL=$1
  PAT=$2
  USERNAME=$3

  RESPONSE=$(
    curl -sS -X POST "$INSTANCE_URL/management/v1/users/machine" \
      -H "Authorization: Bearer $PAT" \
      -H 'Content-Type: application/json' \
      -H 'Accept: application/json' \
      --data-raw '{
         "userName": "'"$USERNAME"'",
         "name": "'"$USERNAME"'",
         "description": "'"$USERNAME"'",
         "accessTokenType": "ACCESS_TOKEN_TYPE_BEARER"
      }'
  )

  PARSED_RESPONSE=$(echo "$RESPONSE" | jq -r '.userId')
  handle_zitadel_request_response "$PARSED_RESPONSE" "create_service_user" "$RESPONSE"
  echo "$PARSED_RESPONSE"
}

create_service_user_secret() {
  INSTANCE_URL=$1
  PAT=$2
  USER_ID=$3

  RESPONSE=$(
    curl -sS -X PUT "$INSTANCE_URL/management/v1/users/$USER_ID/secret" \
      -H "Authorization: Bearer $PAT" \
      -H "Content-Type: application/json" \
      -d '{}'
  )
  SERVICE_USER_CLIENT_ID=$(echo "$RESPONSE" | jq -r '.clientId')
  handle_zitadel_request_response "$SERVICE_USER_CLIENT_ID" "create_service_user_secret_id" "$RESPONSE"
  SERVICE_USER_CLIENT_SECRET=$(echo "$RESPONSE" | jq -r '.clientSecret')
  handle_zitadel_request_response "$SERVICE_USER_CLIENT_SECRET" "create_service_user_secret" "$RESPONSE"
}


delete_admin_service_user() {
  INSTANCE_URL=$1
  PAT=$2

  RESPONSE=$(
    curl -sS -X GET "$INSTANCE_URL/auth/v1/users/me" \
      -H "Authorization: Bearer $PAT" \
      -H "Content-Type: application/json" \
  )
  USER_ID=$(echo "$RESPONSE" | jq -r '.user.id')
  handle_zitadel_request_response "$USER_ID" "delete_auto_service_user_get_user" "$RESPONSE"

  RESPONSE=$(
      curl -sS -X DELETE "$INSTANCE_URL/admin/v1/members/$USER_ID" \
        -H "Authorization: Bearer $PAT" \
        -H "Content-Type: application/json" \
  )
  PARSED_RESPONSE=$(echo "$RESPONSE" | jq -r '.details.changeDate')
  handle_zitadel_request_response "$PARSED_RESPONSE" "delete_auto_service_user_remove_instance_permissions" "$RESPONSE"

  RESPONSE=$(
      curl -sS -X DELETE "$INSTANCE_URL/management/v1/orgs/me/members/$USER_ID" \
        -H "Authorization: Bearer $PAT" \
        -H "Content-Type: application/json" \
  )

  PARSED_RESPONSE=$(echo "$RESPONSE" | jq -r '.details.changeDate')
  handle_zitadel_request_response "$PARSED_RESPONSE" "delete_auto_service_user_remove_org_permissions" "$RESPONSE"
  echo "$PARSED_RESPONSE"
}

installZitadel() {
  if [ -f "$SECRETS_FOLDER/zitadel_enabled" ]; then
    echo "Zitadel is already installed"
    exit 0
  fi

  ensure_curl
  ensure_jq

  ZITADEL_DEV_MODE=true

  MACHINEKEY_TOKEN_PATH=$MACHINEKEY_FOLDER/zitadel-admin-sa.token
  wait_for_file "$MACHINEKEY_TOKEN_PATH"
  MACHINEKEY_TOKEN=$(cat "$MACHINEKEY_TOKEN_PATH")

  if [ -z "$MACHINEKEY_TOKEN" ]; then
    echo "MACHINEKEY_TOKEN is empty"
    exit 1
  fi

  printf "Waiting for Zitadel to become ready "
  wait_api "$ZITADEL_URL" "$MACHINEKEY_TOKEN"

  echo "Creating DataLens project"
  PROJECT_ID=$(create_new_project "$ZITADEL_URL" "$MACHINEKEY_TOKEN" "DataLens")
  set_secret "DL_PROJECT_ID" "$PROJECT_ID"

  echo "Creating Charts application"
  create_new_application "$ZITADEL_URL" \
    "$MACHINEKEY_TOKEN" \
    "Charts" \
    "$DATALENS_UI_URL/api/auth/callback" \
    "$DATALENS_UI_URL/auth" \
    "$ZITADEL_DEV_MODE"

  set_secret "DL_CLIENT_ID" "$APP_CLIENT_ID"
  set_secret "DL_CLIENT_SECRET" "$APP_CLIENT_SECRET"

  echo "Creating charts service user"
  SERVICE_USER_CLIENT_ID="charts"
  MACHINE_USER_ID=$(create_service_user "$ZITADEL_URL" "$MACHINEKEY_TOKEN" "$SERVICE_USER_CLIENT_ID")
  create_service_user_secret "$ZITADEL_URL" "$MACHINEKEY_TOKEN" "$MACHINE_USER_ID"
  set_secret "CHARTS_SERVICE_CLIENT_ID" "$SERVICE_USER_CLIENT_ID"
  set_secret "CHARTS_SERVICE_CLIENT_SECRET" "$SERVICE_USER_CLIENT_SECRET"

  echo "Creating bi service user"
  SERVICE_USER_CLIENT_ID="bi"
  MACHINE_USER_ID=$(create_service_user "$ZITADEL_URL" "$MACHINEKEY_TOKEN" "$SERVICE_USER_CLIENT_ID")
  create_service_user_secret "$ZITADEL_URL" "$MACHINEKEY_TOKEN" "$MACHINE_USER_ID"
  set_secret "BI_SERVICE_CLIENT_ID" "$SERVICE_USER_CLIENT_ID"
  set_secret "BI_SERVICE_CLIENT_SECRET" "$SERVICE_USER_CLIENT_SECRET"

  set_secret "zitadel_enabled" "true"
}

installZitadel