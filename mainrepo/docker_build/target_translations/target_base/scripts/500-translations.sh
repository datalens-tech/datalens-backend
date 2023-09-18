#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Installing translation utils...'


apt-get update

apt-get install --yes \
  gettext
