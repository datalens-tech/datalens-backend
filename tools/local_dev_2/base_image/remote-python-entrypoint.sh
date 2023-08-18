#!/usr/bin/env bash

set -Eeuo pipefail

error_exit() {
    printf "ERROR: %s\n" "$1"
    exit 1
}

INITIAL_LOCAL_INSTALL_MARK="/venv/_INITIAL_LOCAL_INSTALL_MARK"


if [ ! -f "$INITIAL_LOCAL_INSTALL_MARK" ]; then
    bash /local_install.sh
fi

. /venv/bin/activate

if [ -z "${SSH_AUTH_SOCK:-}" ]; then
    SSH_AUTH_SOCK="$(python /find_ssh_agent_socket.py)"
    export SSH_AUTH_SOCK
fi


exec "$@"

