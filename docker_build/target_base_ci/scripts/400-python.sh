#!/usr/bin/env bash

set -exu
# set noninteractive installation
export DEBIAN_FRONTEND=noninteractive

add-apt-repository -y ppa:deadsnakes/ppa

for y in $(seq 1 5)
do
   apt-get update || sleep "$y" ;
done

apt-get install -y python3.12 python3.12-dev python3.12-venv || sleep "$y" ;

ln -sf python3.12 /usr/bin/python && ln -sf python3.12 /usr/bin/python3

curl -sS https://bootstrap.pypa.io/get-pip.py | python3
python3 -m pip install --upgrade pip
