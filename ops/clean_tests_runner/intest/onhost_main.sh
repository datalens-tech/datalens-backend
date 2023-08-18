#!/bin/sh

set -eu
set -x

REPO_PATH="datalens/backend/ops/bi_integration_tests"
BI_ENVS_NAME="int-testing"
CMD="ya make -ttP --test-disable-timeout --test-stderr --test-type pytest"
REVISION="trunk"

while [ -n "${1:-}" ]; do
    case "$1" in
        -e | --environmentS)
            BI_ENV_NAMES="$2"
            shift 2
            ;;
        -c | --cmd)
            CMD="$2"
            shift 2
            ;;
        -r | --repo-path)
            REPO_PATH="$2"
            shift 2
            ;;
        -m | --run-mat)
            export RUN_TESTS_WITH_MATERIALIZATION="$2"
            shift 2
            ;;
        --rev)
            REVISION="$2"
            shift 2
            ;;
        -*)
            printf "ERROR: unknown parameter: %s\n" "$1" >&2
            exit 1
            ;;
        *)
            printf "ERROR: no positional parameters accepted here.\n" >&2
            exit 1
            ;;
    esac
done

export BI_ENVS_NAME


# set +a
set +x
. ./.env
set -x
# set -a

export YAV_TOKEN

# Ensure there's an ssh-agent with a key,
# but do not start a new one each time.
mkdir -p ~/.ssh
chmod 700 ~/.ssh
(
    set +x
    printf "%s\n" "$SEC_SSH_KEY" > ~/.ssh/id_rsa
)
chmod 600 ~/.ssh/id_rsa
touch ~/.ssh_agent
. ~/.ssh_agent
if ! ssh-add -l &>/dev/null; then
    ssh-agent > ~/.ssh_agent
    . ~/.ssh_agent
    ssh-add
fi


# ####### arc #######

mkdir -p ~/.arc
chmod 700 ~/.arc
touch ~/.arc/token
chmod 600 ~/.arc/token
(
    set +x
    printf "%s" "$ARC_TOKEN" > ~/.arc/token
)

mkdir -p ~/arc
cd ~/arc
mkdir -p arcadia store
if [ ! -e arcadia/ya ]; then
    arc mount --allow-other --mount arcadia --store store
fi
cd arcadia
arc fetch trunk
arc pull trunk
arc checkout $REVISION
cd "$REPO_PATH"

export RUN_DEVHOST_TESTS=1
if [ ! -z "${BI_ENV_NAMES}" ]; then
    export DL_ENV_TESTS_FILTER="${BI_ENV_NAMES}"
fi

echo "CMD: ${CMD}"
exec $CMD
