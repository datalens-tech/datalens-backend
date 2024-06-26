#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

mkdir /venv
python -m venv /venv
source /venv/bin/activate

# TODO FIX: pyproject-hooks fix version: https://github.com/python-poetry/poetry/issues/9351
pip install --no-input poetry pyproject-hooks

cd /src/metapkg
poetry export --without=dev --without=ci --without-hashes --format=requirements.txt > requirements.txt
pip install -r requirements.txt
