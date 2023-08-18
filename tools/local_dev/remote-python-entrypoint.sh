#!/usr/bin/env bash

set -Eeuo pipefail

error_exit() {
    printf "ERROR: %s\n" "$1"
    exit 1
}

# Might be non-useful by now.
INIT_DB_AWAIT_TIMEOUT="${INIT_DB_AWAIT_TIMEOUT:-600}"
INIT_DB_HOST="${INIT_DB_HOST:-127.0.0.1}"
INIT_DB_READY_FLAG_PORT="${INIT_DB_READY_FLAG_PORT:-}"

# Path where '$ARC_ROOT/datalens/backend' is mounted to container
PYTHON_PKG_ROOT="${PYTHON_PKG_ROOT:-/data}"

VENV_LOCATION="${VENV_LOCATION:-}"
SKIP_DEPS_INSTALL="${SKIP_DEPS_INSTALL:-}"

if [ -z "$VENV_LOCATION" ]; then
    error_exit "VENV_LOCATION environment variable was not set."
fi

_VENV_WAS_INSTALLED=
if [ ! -d "${VENV_LOCATION}" ]; then
    echo "Creating a new virtualenv at ${VENV_LOCATION}"
    python -m venv "${VENV_LOCATION}"
    _VENV_WAS_INSTALLED=1
fi

. "${VENV_LOCATION}/bin/activate"

if [ -z "${SKIP_DEPS_INSTALL}" ] || [ "${SKIP_DEPS_INSTALL}" = "0" ]; then
    echo "Installing package with development dependencies"
    (
        cd /bitools/requirements && \
        PYTHON_PKG_ROOT="$PYTHON_PKG_ROOT" \
        BI_REQSDIR=/bitools/requirements \
        bash install_all_alt.sh
    )
else
    echo "Skipping dependencies installation (SKIP_DEPS_INSTALL=${SKIP_DEPS_INSTALL})"
fi

# TODO?: remove because initdb-wait logic is in test fixtures now
if [ -n "${INIT_DB_READY_FLAG_PORT}" ]; then
    wait-for-it.sh \
        "${INIT_DB_HOST}:${INIT_DB_READY_FLAG_PORT}" \
        -t "${INIT_DB_AWAIT_TIMEOUT}" \
        || error_exit "Error during awaiting DB init service"
    echo "Init DB ready"
fi

if [ -z "${SSH_AUTH_SOCK:-}" ]; then
    SSH_AUTH_SOCK="$(find_ssh_agent_socket.py)"
    export SSH_AUTH_SOCK
fi

exec "$@"
