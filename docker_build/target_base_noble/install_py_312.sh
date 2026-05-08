#!/usr/bin/env bash

set -exu

apt-get update

apt-get install -y python3.12 python3.12-dev python3.12-venv pipx python3-jinja2

ln -sf python3.12 /usr/bin/python && ln -sf python3.12 /usr/bin/python3
