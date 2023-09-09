#!/usr/bin/env bash

set -exu

add-apt-repository -y ppa:deadsnakes/ppa

for y in $(seq 1 5)
do
   apt-get update || sleep "$y" ;
done

apt-get install -y python3.10 python3.10-dev python3-pip python3.10-venv || sleep "$y" ;


ln -sf python3.10 /usr/bin/python && ln -sf python3.10 /usr/bin/python3
