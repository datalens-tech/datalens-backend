#!/bin/bash
set -e

# Start YDB
sh ./initialize_local_ydb &

# Start server to access certificate files
python3 -m http.server -d /ydb_certs 51904 &

wait -n

exit $?

tail -f /dev/null
