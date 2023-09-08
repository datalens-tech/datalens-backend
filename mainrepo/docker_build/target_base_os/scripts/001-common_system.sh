#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Installing bunch of system utils ...'
apt-get install --yes \
  apt-utils \
  apt-transport-https \
  ca-certificates \
  dirmngr \
  curl \
  wget
