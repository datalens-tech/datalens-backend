#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Installing docker packages...'

echo '  Installing docker source list...'
apt-get install --yes \
  curl \
  gpg \
  ca-certificates

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian/ bookworm stable" > /etc/apt/sources.list.d/docker.list

apt-get update

echo '  Installing packages...'

apt-get install --yes \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin
