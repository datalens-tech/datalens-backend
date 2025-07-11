#!/usr/bin/env bash

set -exu

echo 'deb http://ppa.launchpad.net/deadsnakes/ppa/ubuntu noble main' > /etc/apt/sources.list.d/deadsnakes.list

apt-get update

apt-get install -y python3.10 libpython3.10 || sleep "$y" ;

ln -sf python3.10 /usr/bin/python && ln -sf python3.10 /usr/bin/python3
