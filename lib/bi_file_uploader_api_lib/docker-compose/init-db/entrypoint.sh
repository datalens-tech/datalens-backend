#!/bin/bash

BEACON_WS_ROOT=/beacon_ws_root/

chmod +x /s3/docker-entrypoint-initdb.d/*
run-parts -v --regex ".\\.sh$" /s3/docker-entrypoint-initdb.d

echo "Starting beacon HTTP server"

mkdir -p ${BEACON_WS_ROOT}
cd ${BEACON_WS_ROOT}

# Tests will wait for this port before launch
python3 -m http.server 8000
