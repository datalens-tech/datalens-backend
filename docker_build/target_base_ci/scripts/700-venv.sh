#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

mkdir /venv
python -m venv /venv
source /venv/bin/activate

pip install --no-input poetry==1.5.0

cd /src/metapkg
poetry export --without=dev --without=ci --without-hashes --format=requirements.txt > requirements.txt
pip install -r requirements.txt
