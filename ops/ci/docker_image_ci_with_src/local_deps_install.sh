#!/usr/bin/env bash

set -exu

set -x; TMP_DIR=`shuf -er -n20  {A..Z} {a..z} {0..9} | tr -d '\n'`
mkdir -p /tmp/$TMP_DIR

source /venv/bin/activate
cd /data/ops/ci
pip install poetry==1.5.0
poetry export --without-hashes --with=dev --with=ci --format=requirements.txt > /tmp/$TMP_DIR/requirements.txt
cat /tmp/$TMP_DIR/requirements.txt
LOCAL_DEPS=$(cat /tmp/$TMP_DIR/requirements.txt | grep "file:///"  | cut -d"@" -f2 | cut -d";" -f1 | tr "\n" " " )

pip install --no-deps --no-build-isolation file:///data/ops/ci $LOCAL_DEPS
