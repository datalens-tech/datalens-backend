#!/bin/bash -x

# #######
# Given a ssh target ('root@hostname'), run bi integrational tests on it.
# #######

set -Eeuo pipefail

target="$1"
shift 1

yav-deploy --debug -c ../.secrets/ -d ./

# Extra sandbox support
if [ x"$(whoami)" = x"sandbox" ] && [ ! -f ~/.ssh/id_rsa ]; then
    mkdir -p ~/.ssh
    chmod 700 ~/.ssh
    (
        set +x
        . ./.env
        printf "%s\n" "$SEC_SSH_KEY" > ~/.ssh/id_rsa
    )
    chmod 600 ~/.ssh/id_rsa
fi

export REPO_URL REPO_BRANCH CMD EXTRADBG DOCKER_USERNAME


if [ "${INTEST_RECREATE_VM:-}" ]; then
    if [ x"$target" != x"root@bi-intest.sas.yp-c.yandex.net" ]; then
        printf "Cannot recreate with target other than 'root@bi-intest.sas.yp-c.yandex.net': %s\n" "$target" >&2
        exit 1
    fi
    if [ ! -f './vmctl' ]; then
        # local status
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
    (
        set +x
        . ./.env
        export VMCTL_TOKEN
        ./vmctl deallocate -y --pod-id=bi-intest --cluster=SAS || true
        ./vmctl \
            create \
            --pod-id=bi-intest \
            --cluster=SAS \
            --abc=2170 \
            --groups=84915 \
            --logins robot-datalens \
            --node-segment=dev \
            --image="rbtorrent:133f5140f00a5cead1a5e349850cbdd9011c62b0" \
            --image-type=RAW \
            --type linux \
            --storage=ssd \
            --volume-size=50G \
            --memory=4G \
            --network-id=_DL_BACKEND_DEV_NETS_ \
            --vcpu-guarantee=1 \
            --vcpu-limit=4 \
            --io-guarantee-ssd=30M \
            --io-guarantee-hdd=0 \
            --use-nat64 \
            -y \
            --wait \
            >&2
    )
fi


# Or: `tar -cf - ./main.sh ./prepare.sh | ssh "$target" 'tar -xf && ./prepare.sh && ./main.sh'
scp -v -C ./onhost_main.sh ./.env "$target":

quote(){
    # Could be done (in bash) as `printf "%q" "$1"`,
    # but this way leads to a more readable result.
    printf "%s" "$1" \
        | sed -zr '
            s/\\/\\\\/g;
            s/"/\\"/g;
            s/^/"/;
            s/$/"/
        '
}

cmd="./onhost_main.sh "
for arg in "$@"; do
    cmd="$cmd $(quote "$arg")"
done
exec ssh "$target" "$cmd"
