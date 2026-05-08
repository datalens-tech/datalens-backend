#!/usr/bin/env bash

set -exu

apt-get update

apt-get install -y python3.12 libpython3.12 python3.12-venv

ln -sf python3.12 /usr/bin/python && ln -sf python3.12 /usr/bin/python3
