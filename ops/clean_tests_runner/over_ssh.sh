#!/bin/bash -x

# #######
# Given a ssh target ('root@hostname'), run bi tests on it.
#
# WARNING: the target should not be a persistent host (force apt installs and
# force docker rm will be done on it).
#
# Alternatively, use `./over_ssh.sh provision` to make a new target on QYP.
#
# Note: '84915' is 'svc_yandexbi_development':
# https://staff-api.yandex-team.ru/v3/groups?url=svc_yandexbi_development&_one=1
# https://staff-api.yandex-team.ru/v3/groups?id=84915&_one=1
# #######

set -Eeuo pipefail

DC="${PRIVISION_DC:-sas}"
# Ensure lowercase
DC="$(printf '%s' "$DC" | tr '[:upper:]' '[:lower:]')"


_provision_request_vmctl() {
    set -Eeuo pipefail

    local dc
    dc="$1"
    # Ensure uppercase (separately)
    dc_uc="$(printf '%s' "$dc" | tr '[:lower:]' '[:upper:]')"
    local name
    name="$2"
    if [ ! -f './vmctl' ]; then
        local status
        status="$(
        curl \
            -s \
            --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 180 \
            -w '%{http_code}' \
            -o ./vmctl \
            'https://proxy.sandbox.yandex-team.ru/last/VMCTL_LINUX'
        )"
        if [ x"$status" != x"200" ]; then
            printf "Non-ok download-sandbox-vmctl response: %s\n" "$status" >&2
            exit 1
        fi
        chmod +x ./vmctl
    fi

    maybe_whoami="$(whoami)"
    if [ x"$maybe_whoami" = x"sandbox" ]; then
        maybe_whoami=""
    fi

    # Base image variations:
    #   * `--default-image=bionic`  # will require `FORCE_PREPARE=1`
    #   * pre-prepared image: https://sandbox.yandex-team.ru/resource/1568307785/view

    # abc=2170: https://staff-api.yandex-team.ru/v3/groups?_one=1&url=svc_yandexbi
    # groups=84915: https://staff-api.yandex-team.ru/v3/groups?_one=1&url=svc_yandexbi_development

    ./vmctl \
        create \
        --pod-id="$name" \
        --cluster="$dc_uc" \
        --abc=2170 \
        --groups=84915 \
        --logins robot-datalens $maybe_whoami \
        --node-segment=dev \
        --image="rbtorrent:133f5140f00a5cead1a5e349850cbdd9011c62b0" \
        --image-type=RAW \
        --io-guarantee-ssd=30M \
        --type linux \
        --storage=ssd \
        --volume-size=90G \
        --memory=12G \
        --network-id=_DL_BACKEND_DEV_NETS_ \
        --vcpu-guarantee=1 \
        --vcpu-limit=4 \
        --use-nat64 \
        -y \
        --wait \
        >&2
}


target="$1"

yav-deploy --debug -c ./.secrets/ -d ./

# Primarily for the VMCTL_TOKEN
set +x
. ./.env
set -x

# Extra sandbox support
if [ x"$(whoami)" = x"sandbox" ] && [ ! -f ~/.ssh/id_rsa ]; then
    mkdir -p ~/.ssh
    chmod 700 ~/.ssh
    set +x
    printf "%s\n" "$SEC_SSH_KEY" > ~/.ssh/id_rsa
    set -x
    chmod 600 ~/.ssh/id_rsa
fi

if [ x"$target" = x"provision" ]; then
    name="bitst$(date '+%s%N')"
    # name="bitst$(date '+%Y_%m_%d_%H_%M_%S_%N')"  # "id length should be less than 30, got 34"
    dc="$DC"

    hostname="${name}.${dc}.yp-c.yandex.net"
    target="root@${hostname}"
    printf "target='%s';\n" "$target" >&2

    _provision_request_vmctl "$dc" "$name"

    dc_uc="$(printf '%s' "$dc" | tr '[:lower:]' '[:upper:]')"
    if [ ! -z "${DELETE_AFTERWARDS:-}" ]; then
        trap './vmctl deallocate -y --pod-id="$name" --cluster="$dc_uc"' EXIT
    else
        # Convenience:
        trap 'printf "To deallocate:    ./vmctl deallocate -y --pod-id='\''%s'\'' --cluster='\''%s'\''\n" "$name" "$dc_uc"' EXIT
    fi

    max_attempts=600
    sleep_time=1
    for attempt in $(seq 1 "$max_attempts"); do
        if ! ssh "$target" true; then
            if [ "$attempt" -eq "$max_attempts" ]; then
                printf "Timed out while waiting for the target to come up (%s x %s seconds).\n" "$max_attempts" "$sleep_time" >&2
                exit 1
            fi
            printf "Waiting for the target to come up (@%s x %ss)...\n" "$attempt" "$sleep_time" >&2
            sleep "$sleep_time"
            continue
        fi
        break
    done
fi


# REPO_URL="ssh://git@bb.yandex-team.ru/statbox/bi-common.git"
# REPO_URL="ssh://git@bb.yandex-team.ru/statbox/bi-formula.git"
# REPO_URL="ssh://git@bb.yandex-team.ru/statbox/bi-api.git"
# REPO_URLS="ssh://git@bb.yandex-team.ru/statbox/bi-common.git ssh://git@bb.yandex-team.ru/statbox/bi-formula.git ssh://git@bb.yandex-team.ru/statbox/bi-converter-api.git ssh://git@bb.yandex-team.ru/statbox/bi-billing.git ssh://git@bb.yandex-team.ru/statbox/bi-uploads.git ssh://git@bb.yandex-team.ru/statbox/bi-materializer.git ssh://git@bb.yandex-team.ru/statbox/bi-api.git"

# REPO_BRANCH="master"
# REPO_BRANCH="testenvfix"

# CMD="make test"
# EXTRADBG=1
# DOCKER_USERNAME="robot-statface-api"
export REPO_URL REPO_BRANCH CMD EXTRADBG DOCKER_USERNAME

# Or: `tar -cf - ./main.sh ./prepare.sh | ssh "$target" 'tar -xf && ./prepare.sh && ./main.sh'
scp -v -C ./prepare.sh ./main.sh ./.env "$target":
if [ ! -z "${FORCE_PREPARE:-}" ]; then
    ssh "$target" ./prepare.sh
fi
if [ -z "${REPO_URLS:-}" ]; then
    REPO_URLS="${REPO_URL:-ssh://git@bb.yandex-team.ru/statbox/bi-api.git}"
else
    if [ ! -z "${REPO_URL:-}" ]; then
        echo "Do not combine REPO_URL and REPO_URLS." >&2
        exit 1
    fi
fi
for REPO_URL in $REPO_URLS; do

printf "\n\n\n ======= ======= ======= REPO_URL='%s' ======= ======= =======\n" "$REPO_URL"

ssh "$target" '
    set -e
    set +a
    . ./.env
    REPO_URL="'"${REPO_URL:-}"'"
    REPO_BRANCH="'"${REPO_BRANCH:-}"'"
    CMD="'"${CMD:-}"'"
    export REPO_URL REPO_BRANCH CMD SEC_SSH_KEY SEC_REGISTRY_AUTH YAV_TOKEN
    cd "$(mktemp -d)"
    exec ~/main.sh
'

done
