#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Installing docker packages...'

mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable" > /etc/apt/sources.list.d/docker.list
rm /etc/apt/sources.list.d/clickhouse.list


apt-get update
# apt-get install --yes  docker-ce-cli docker-compose-plugin
#  it seems we need docker binary for docker action in github actions
apt-get install --yes \
  docker-ce \
  docker-ce-cli \
  containerd.io \
  docker-compose-plugin
