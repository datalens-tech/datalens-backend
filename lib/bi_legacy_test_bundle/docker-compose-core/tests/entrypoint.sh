#!/bin/bash -x

# Docker DNS timeout hack
# (to account for builtin docker DNS server that does not respond at all to AAAA queries)
if ! grep -q 'options timeout' /etc/resolv.conf; then
    echo 'options timeout:1' >> /etc/resolv.conf
fi

INIT_DB_HOST=${INIT_DB_HOST:=init-db}
INIT_DB_READY_FLAG_PORT=${INIT_DB_READY_FLAG_PORT:=8000}
INIT_DB_AWAIT_TIMEOUT=${INIT_DB_AWAIT_TIMEOUT:=600}

exit_on_error() {
    echo $1
    exit -1
}

wait-for-it.sh ${INIT_DB_HOST}:${INIT_DB_READY_FLAG_PORT} -t ${INIT_DB_AWAIT_TIMEOUT} || exit_on_error "Error during awaiting DB init service"
echo "Init DB ready"

exec "$@"
