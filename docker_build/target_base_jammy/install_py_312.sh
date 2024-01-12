#!/usr/bin/env bash

set -exu

echo 'deb http://ppa.launchpad.net/deadsnakes/ppa/ubuntu jammy main' > /etc/apt/sources.list.d/deadsnakes.list

apt-get update

apt-get install -y python3.12 python3.12-dev python3.12-venv || sleep "$y" ;

ln -sf python3.12 /usr/bin/python && ln -sf python3.12 /usr/bin/python3
