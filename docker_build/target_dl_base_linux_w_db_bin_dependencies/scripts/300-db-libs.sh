#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

apt-get install --yes \
  libpq-dev \
  clickhouse-client
