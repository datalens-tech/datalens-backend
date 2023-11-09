#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

apt-get install --yes --fix-missing \
  freetds-dev tdsodbc unixodbc unixodbc-dev libtcmalloc-minimal4  # For MSSQL
