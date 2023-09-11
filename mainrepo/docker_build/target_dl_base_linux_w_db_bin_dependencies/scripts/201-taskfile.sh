#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Installing taskfile tool...'


TMP_DIR="$(mktemp -d)"

cd $TMP_DIR
wget https://github.com/go-task/task/releases/download/v3.29.1/task_linux_amd64.deb
echo "e411770abf73d5e094100ab7a1c8278f35b591ecadbfd778200b6b2ad1ee340b task_linux_amd64.deb" | sha256sum -c -
dpkg -i task_linux_amd64.deb

rm -r "$TMP_DIR"
