#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

# For MSSQL
# temporarily pinned (see https://github.com/datalens-tech/datalens-backend/issues/201)
apt-get install --yes --fix-missing \
    freetds-dev \
    tdsodbc \
    unixodbc  \
    unixodbc-dev  \
    libtcmalloc-minimal4
