#! /usr/bin/env bash

set -ex pipefail

SECRETS_FOLDER="/data/secrets"
MACHINEKEY_FOLDER="/data/machinekey"


ensure_openssl() {
  if ! command -v openssl &> /dev/null; then
    apt-get update
    apt-get install -y openssl
  fi
}


ensure_folder() {
  FOLDER=$1

  if [ ! -d "$FOLDER" ]; then
    mkdir -p "$FOLDER"
  fi
}

ensure_masterkey() {
  MASTERKEY_FILE="$SECRETS_FOLDER/zitadel_masterkey"
  if [ ! -f "$MASTERKEY_FILE" ]; then
    ensure_openssl
    openssl rand -base64 32 | head -c 32 > "$MASTERKEY_FILE"
  fi
}


main() {
  ensure_folder "$SECRETS_FOLDER"
  ensure_folder "$MACHINEKEY_FOLDER"
  ensure_masterkey
}

main
