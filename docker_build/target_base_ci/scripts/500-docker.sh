#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Installing docker packages...'

mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
#chmod 755 /etc/apt/keyrings
#chmod a+r /etc/apt/keyrings/docker.gpg

case $(arch) in
"aarch64"|"arm64")
    arch="arm64"
    ;;
*)
    arch="amd64"
    ;;
esac
echo "deb [arch=${arch} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu jammy stable" > /etc/apt/sources.list.d/docker.list

apt-get update
# apt-get install --yes  docker-ce-cli docker-compose-plugin
#  it seems we need docker binary for docker action in github actions


apt-get install --yes \
  docker-ce \
  docker-ce-cli \
  containerd.io \
  docker-compose-plugin
