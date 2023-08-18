#!/usr/bin/env bash

#!/usr/bin/env bash

set -eux

TARGET_PATH=$1

# from ${job.container.network}"
NET_NAME=$2
export NET_NAME
COMPOSE_PROJECT_NAME=$3
export COMPOSE_PROJECT_NAME

target_root_path="/data/$(echo $TARGET_PATH | cut -d ':' -f1)/"
cd "$target_root_path"

echo "Running py tests for $TARGET_PATH"

source /venv/bin/activate

if [ -f _gh_ci_fix.sh ]; then
    echo "Executing workaround script"
    bash _gh_ci_fix.sh
fi

# todo: remove local compose after migration from arc
local_compose_file="$(get_compose_path '/data' $TARGET_PATH with_local_suffix )"
main_compose_file="$(get_compose_path '/data' $TARGET_PATH )"

if [ "$(do_we_need_compose "/data" $TARGET_PATH)" == "1" ]; then

    # Use local compose until we leave arcadia ci
    if [ -f $local_compose_file ]; then
        cp -f $local_compose_file $main_compose_file
    fi

    # Add network config to the compose file
    cat /data/ops/ci/networks_addon.yml >> $main_compose_file
    cat $main_compose_file

    fix_ports_in_compose $main_compose_file "docker-compose.fixed.yml"
    cat "docker-compose.fixed.yml"

    # Copy .env if available
    if [ -f "${target_root_path}github/.env" ]; then
      cp "${target_root_path}github/.env" "${target_root_path}.env"
    fi

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

TEST_CONTAINER_HOST=from_compose PYTHONUNBUFFERED=1 run_tests "$TARGET_PATH"
