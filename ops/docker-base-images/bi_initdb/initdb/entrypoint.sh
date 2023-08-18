#!/bin/sh -x

set -Eeu

BEACON_WS_ROOT=/opt/initdb/beacon_ws_root/

chmod +x /opt/initdb/mssql/docker-entrypoint-initdb.d/*
run-parts -v --regex ".\\.sh$" /opt/initdb/mssql/docker-entrypoint-initdb.d

chmod +x /opt/initdb/oracle/docker-entrypoint-initdb.d/*
run-parts -v --regex ".\\.sh$" /opt/initdb/oracle/docker-entrypoint-initdb.d

echo "Starting beacon HTTP server"

mkdir -p ${BEACON_WS_ROOT}
cd ${BEACON_WS_ROOT}

# Tests will wait for this port before launch
if [ -z "${INITDB_DISABLE_BEACON_SERVER:-}" ]; then
    # essentially serves the 'directory listing' of an empty directory.
    exec python3 -m http.server 8000
else
    exec "$@"
fi
