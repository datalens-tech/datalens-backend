#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

# For MSSQL
apt-get install --yes --fix-missing \
    freetds-dev \
    tdsodbc \
    unixodbc  \
    unixodbc-dev  \
    libtcmalloc-minimal4
