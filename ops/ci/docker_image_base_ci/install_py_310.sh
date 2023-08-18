#!/usr/bin/env bash

set -exu

# yep, we override python3.9 from /trunk/arcadia/datalens/backend/ops/docker-base-images/base_ubuntu_runit/Dockerfile
# by python3.10 from ppa
# beacause there is no python3.10 in yandex mirror for ubuntu 20.04
echo 'deb http://ppa.launchpad.net/deadsnakes/ppa/ubuntu focal main' > /etc/apt/sources.list.d/deadsnakes.list
# fix source for clickhouse
echo 'deb https://repo.yandex.ru/clickhouse/deb/dists/ stable main' >> /etc/apt/sources.list.d/clickhouse.list

for y in $(seq 1 2)
do
   apt-get update || sleep "$y" ;
done

apt-get install -y python3.10 python3.10-dev python3-pip python3.10-venv || sleep "$y" ;


ln -sf python3.10 /usr/bin/python && ln -sf python3.10 /usr/bin/python3
