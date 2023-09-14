#!/usr/bin/env bash

#!/usr/bin/env bash

set -ux

TARGET_PATH=$1

# from ${job.container.network}"
NET_NAME=$2
export NET_NAME

COMPOSE_PROJECT_NAME=$3
export COMPOSE_PROJECT_NAME

target_root_path="/data/$(echo $TARGET_PATH | cut -d ':' -f1)/"

source /venv/bin/activate
if [ "$(do-we-need-compose "/data" $TARGET_PATH)" == "1" ]; then
    cd "$target_root_path" || exit 3
    docker compose  -f "docker-compose.fixed.yml" rm --stop --volumes
    sleep 2
    docker compose  -f "docker-compose.fixed.yml" rm --stop --force --volumes
fi
