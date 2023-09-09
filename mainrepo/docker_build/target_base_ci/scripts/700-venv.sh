#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

mkdir /venv
python -m venv /venv
source /venv/bin/activate

pip install --no-input poetry==1.5.0

cd /src/metapkg
poetry install --no-root --without=dev --without=ci

