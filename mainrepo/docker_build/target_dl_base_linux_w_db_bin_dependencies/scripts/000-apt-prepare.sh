#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Updating apt-get...'

apt-get update

echo 'Upgrading apt-get ...'

apt-get full-upgrade --yes
