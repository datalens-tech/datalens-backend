#!/usr/bin/env bash

set -eux

TARGET_PATH=$1
NET_NAME=$2
export NET_NAME
COMPOSE_PROJECT_NAME=$3
export COMPOSE_PROJECT_NAME

target_root_path="/src/$(echo $TARGET_PATH | cut -d ':' -f1)/"
cd "$target_root_path"

echo "Running py tests for $TARGET_PATH"

source /venv/bin/activate


main_compose_file="$(get_compose_path '/src' $TARGET_PATH )"

if [ "$(do_we_need_compose "/src" $TARGET_PATH)" == "1" ]; then

    # Add network config to the compose file
    cat /src/ci/networks_addon.yml >> $main_compose_file
    cat $main_compose_file

    # Fix ports from used in local dev to native service ones
    fix_ports_in_compose $main_compose_file "docker-compose.fixed.yml"
    cat "docker-compose.fixed.yml"

    docker compose -f "docker-compose.fixed.yml" build --pull
    docker compose -f "docker-compose.fixed.yml" up  --force-recreate -d
fi

# echo "0" > _debug
# echo "now ssh to github runner host and do docker exec -it $container_id bash
# until [[  $(cat _debug) == 1 ]]
# do
#    echo "waiting mark in _debug == 1"
#    sleep 5
# done

set +x
TEST_CONTAINER_HOST=from_compose PYTHONUNBUFFERED=1 env "${@:4}" run_tests /src "$TARGET_PATH"
