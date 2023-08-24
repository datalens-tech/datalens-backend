#!/bin/bash

BEACON_WS_ROOT=/beacon_ws_root/

WAIT_FOR_MSSQL=${WAIT_FOR_MSSQL:-1}
if [ "${WAIT_FOR_MSSQL}" == "1" ]; then
  chmod +x /mssql/docker-entrypoint-initdb.d/*
  run-parts -v --regex ".\\.sh$" /mssql/docker-entrypoint-initdb.d
fi

WAIT_FOR_ORACLE=${WAIT_FOR_ORACLE:-1}
if [ "${WAIT_FOR_ORACLE}" == "1" ]; then
  chmod +x /oracle/docker-entrypoint-initdb.d/*
  run-parts -v --regex ".\\.sh$" /oracle/docker-entrypoint-initdb.d
fi

echo "Starting beacon HTTP server"

mkdir -p ${BEACON_WS_ROOT}
cd ${BEACON_WS_ROOT}

# Tests will wait for this port before launch
python3 -m http.server 8000
