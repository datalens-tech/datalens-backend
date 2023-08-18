#!/usr/bin/env bash

# TODO FIX: Rewrite in Python

CMD="$1"
shift

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
BACKEND_DIR_LOCAL="$(realpath "${HERE}"/../..)"
BACKEND_DIR_IN_CONTAINER="/data"

DOCKER_COMPOSE_FILE="${HERE}/docker-compose.yml"
DC_SERVICE_NAME="pi"

LOG_PREFIX="dl-docker-env"

log_error() {
    echo "${LOG_PREFIX}:ERR: ${1}"
}

print_args() {
    while (( "$#" )); do
        echo "$1"
        shift
    done
}

rel_path_to_backend() {
    realpath --relative-to="${BACKEND_DIR_LOCAL}" "${1}"
}

case "${CMD}" in
    "run")
        DC_RUN_EXTRA_OPTIONS=()
        while (( "$#" )) && [[ "$1" == -* ]]; do
            _FLAG="$1"
            shift

            if [[ "${_FLAG}" == "--" ]]; then
                break
            fi
            case "${_FLAG}" in
                "--set-pwd")
                    DC_RUN_EXTRA_OPTIONS+=("-w")
                    DC_RUN_EXTRA_OPTIONS+=("${BACKEND_DIR_IN_CONTAINER}/$(rel_path_to_backend "${PWD}")")
                ;;
                "-e")
                    DC_RUN_EXTRA_OPTIONS+=("-e")
                    DC_RUN_EXTRA_OPTIONS+=("$1")
                    shift
                ;;
                *)
                    log_error "Unknown flag for command \"run\": \"${_FLAG}\""
                    exit 1;
                ;;
            esac
        done

        if [[ "$#" != "0" ]]; then
            COMMAND_FOR_CONTAINER=("$@")
        else
            COMMAND_FOR_CONTAINER=("bash")
        fi

        docker-compose -f "${DOCKER_COMPOSE_FILE}" run "-e" SKIP_DEPS_INSTALL=1 --rm "${DC_RUN_EXTRA_OPTIONS[@]}" \
          "${DC_SERVICE_NAME}" "${COMMAND_FOR_CONTAINER[@]}"
    ;;
    *)
        log_error "Unknown command: \"${CMD}\""
    ;;
esac
