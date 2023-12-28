#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

# For MSSQL
# temporarily pinned (see https://github.com/datalens-tech/datalens-backend/issues/201)
apt-get install --yes --fix-missing \
    libct4=1.1.6-1.1 \
    libsybdb5=1.1.6-1.1 \
    freetds-dev=1.1.6-1.1 \
    tdsodbc=1.1.6-1.1 \
    unixodbc  \
    unixodbc-dev  \
    libtcmalloc-minimal4
