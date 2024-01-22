#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

mkdir /venv
python -m venv /venv
source /venv/bin/activate

pip install --no-input poetry==1.7.1

cd /src/metapkg

# TODO FIX: Remove pandas install from github repository after pandas removing
mkdir datalens_wheels_latest
curl -sL https://github.com/datalens-tech/datalens-wheels/archive/refs/tags/0.2.0.tar.gz | tar xfz - -C datalens_wheels_latest/
pip install datalens_wheels_latest/datalens-wheels-0.2.0/pandas_binary/pandas-2.0.3-cp312-cp312-linux_x86_64.whl

poetry export --without=dev --without=ci --without-hashes --format=requirements.txt > requirements.txt
pip install -r requirements.txt
