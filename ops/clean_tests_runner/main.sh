#!/bin/sh

if [ -z "${REPO_URL:-}" ]; then REPO_URL="${REPO_URL:-ssh://git@bb.yandex-team.ru/statbox/bi-api.git}"; fi
# REPO_URL="ssh://git@bb.yandex-team.ru/statbox/bi-formula.git"

if [ -z "${REPO_BRANCH:-}" ]; then REPO_BRANCH="${REPO_BRANCH:-master}"; fi
# REPO_BRANCH="testenvfixes"

if [ -z "${CMD:-}" ]; then CMD="${CMD:-make test}"; fi

EXTRADBG="${EXTRADBG:-}"

# TODO: task params:
# type: EXEC_SCRIPT_LXC
# owner: STATFACE
# lxc container resource: 870270439
# # or: latest STATFACE_TASK_RUNNER_LXC
# vault: robot-statinfra-ssh-key : SEC_SSH_KEY
# vault: robot-datalens-yav-oauth : YAV_TOKEN
# vault: robot-datalens-api-qloud : SEC_REGISTRY_AUTH

# export SSH_OPTIONS=UserKnownHostsFile=/dev/null,StrictHostKeyChecking=no

set -e

if [ -z "$SEC_REGISTRY_AUTH" ]; then echo "Missing: \$SEC_REGISTRY_AUTH" >&2; exit 1; fi
if [ -z "$YAV_TOKEN" ]; then echo "Missing: \$YAV_TOKEN" >&2; exit 1; fi
if [ -z "$SEC_SSH_KEY" ]; then echo "Missing: \$SEC_SSH_KEY" >&2; exit 1; fi

if [ "$EXTRADBG" ]; then
    whoami >&2
    df -h >&2
fi

# hax: docker -> sudo
# TODO: put into the base container.
sudo whoami || docker run --rm -v /etc:/upetc ubuntu:bionic bash -c "printf '$(whoami) ALL=(ALL) NOPASSWD: ALL\n' > /upetc/sudoers.d/hax"
sudo whoami

# sandbox-hax: docker over ramdrive (because can't use overlayfs2 here, and
# without it everything is too slow).
if [ -z "$RAMDRIVE_PATH" ]; then
    echo "NOTE: \$RAMDRIVE_PATH is empty."
else

# feature: https://a.yandex-team.ru/review/1030668/details
# reference: https://a.yandex-team.ru/arc/commit/5646338
sudo python3 -c "
import sys
import json
DOCKER_CONFIG_FILE = '/etc/docker/daemon.json'
ramdrive_path = sys.argv[1]
with open(DOCKER_CONFIG_FILE, 'r') as fobj:
    docker_config = json.load(fobj)
docker_config['data-root'] = '{}/docker'.format(ramdrive_path)
with open(DOCKER_CONFIG_FILE, 'w') as fobj:
    json.dump(docker_config, fobj, indent=4)
" "$RAMDRIVE_PATH"
sudo service docker restart
# see also:
# https://github.com/ClickHouse/ClickHouse/blob/master/docker/test/integration/runner/dockerd-entrypoint.sh
max_attempts=50
sleep_time=0.1
for attempt in $(seq 1 "$max_attempts"); do
    if ! docker info; then
        if [ "$attempt" -eq "$max_attempts" ]; then
            printf "Timed out while waiting for docker to come up (%s x %s seconds).\n" "$max_attempts" "$sleep_time" >&2
            exit 1
        fi
        printf "Waiting for docker to come up (@%s x %ss)...\n" "$attempt" "$sleep_time" >&2
        sleep "$sleep_time"
        continue
    fi
    break
done

fi
docker info  # just in case, again

# assuming deb-installed: `git`

# ssh setup
mkdir -p ~/.ssh
chmod 700 ~/.ssh
printf "%s\n" "$SEC_SSH_KEY" > ~/.ssh/id_rsa
chmod 600 ~/.ssh/id_rsa

eval "$(ssh-agent)"
ssh-add
ssh-add -l

# ssh-keyscan -H -t rsa github.yandex-team.ru >> ~/.ssh/known_hosts
ssh-keyscan -H -t rsa bb.yandex-team.ru >> ~/.ssh/known_hosts

if [ "$EXTRADBG" ]; then
    printf "HOME: %s\n" "$HOME" >&2
    printf "key: %s\n" "$(ls -laFd ~/.ssh/id_rsa)" >&2
    printf "ssh cfg: %s\n" "$(cat /etc/ssh/ssh_config)" >&2
    printf "ssh known_hosts: %s\n" "$(cat ~/.ssh/known_hosts)" >&2
fi

# docker registry
DOCKER_USERNAME="${DOCKER_USERNAME:-robot-statface-api}"
printf "%s\n" "$SEC_REGISTRY_AUTH" | docker login --username "$DOCKER_USERNAME" --password-stdin registry.yandex.net

. ~/.env

mkdir -p ~/.arc
chmod 700 ~/.arc
touch ~/.arc/token
chmod 600 ~/.arc/token
printf "%s" "$ARC_TOKEN" > ~/.arc/token


set -x

if [ "$EXTRADBG" ]; then
    ip -6 addr >&2
    ip -6 ro >&2
    docker network ls >&2
fi

# network
# docker system prune -a -f
lst="$(docker ps -a -q)"
if [ -n "$lst" ]; then docker rm -f $lst; fi

# # XXXXXXX: should be in the tested repo
# # network bi-common
# docker network rm bi-common || true
# docker network create --ipv6 --driver bridge --subnet=fcbe:fdea::4:0/112 --gateway=fcbe:fdea::4:1 bi-common
# sudo ip6tables -t nat -D POSTROUTING -s fcbe:fdea::4:0/112 -j MASQUERADE || true
# sudo ip6tables -t nat -A POSTROUTING -s fcbe:fdea::4:0/112 -j MASQUERADE

# # XXXXXXX: should be in the tested repo
# # ??? network bi-api
# docker network rm bi-api || true
# docker network create --ipv6 --driver bridge --subnet=fcbe:fdea::5:0/112 --gateway=fcbe:fdea::5:1 bi-api
# sudo ip6tables -t nat -D POSTROUTING -s fcbe:fdea::5:0/112 -j MASQUERADE || true
# sudo ip6tables -t nat -A POSTROUTING -s fcbe:fdea::5:0/112 -j MASQUERADE


# # XXXXXXX: should be in the tested repo
# # ??? network bi-uploads
# docker network rm bi-uploads || true
# docker network create --ipv6 --driver bridge --subnet=fcbe:fdea::6:0/112 --gateway=fcbe:fdea::6:1 bi-uploads
# sudo ip6tables -t nat -D POSTROUTING -s fcbe:fdea::6:0/112 -j MASQUERADE || true
# sudo ip6tables -t nat -A POSTROUTING -s fcbe:fdea::6:0/112 -j MASQUERADE


# # XXXXXXX: should be in the tested repo
# # ??? network bi-billing
# docker network rm bi-billing || true
# docker network create --ipv6 --driver bridge --subnet=fcbe:fdea::7:0/112 --gateway=fcbe:fdea::7:1 bi-billing
# sudo ip6tables -t nat -D POSTROUTING -s fcbe:fdea::7:0/112 -j MASQUERADE || true
# sudo ip6tables -t nat -A POSTROUTING -s fcbe:fdea::7:0/112 -j MASQUERADE


# TODO: `cat > /etc/apt/trusted.gpg.d/yandex.asc <<...` might work better
# TODO: put it into the base container.
sudo apt-key add - <<\_EOF
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.4.10 (GNU/Linux)

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
zNicZHfu680ocsUdJKoQqA==
=m3IH
-----END PGP PUBLIC KEY BLOCK-----
_EOF
sudo tee /etc/apt/sources.list.d/yandex.list <<\_EOF
deb http://common.dist.yandex.ru/common stable/all/
deb http://common.dist.yandex.ru/common stable/$(ARCH)/
_EOF
if ! which yav; then
    sudo apt-get -y update
    sudo apt-get -y install yandex-passport-vault-client
fi


if ! which arc; then
    sudo apt-get -y update
    sudo apt-get -y install yandex-arc-launcher
fi

# repo

# arc case:
# e.g. "arc:/datalens/backend/bi-uploads" -> "datalens/backend/bi-uploads"
arc_path="$(
    printf "%s\n" "$REPO_URL" | \
    sed -rn 's|^arc:/|| p'
)"
if [ -n "$arc_path" ]; then
    # XXX: temporary. hopefully.
    if [ x"$REPO_BRANCH" = x"master" ]; then
        REPO_BRANCH="trunk"
    fi

    # # Robots are, generally, not allowed such ssh access:
    # svn co \
    #     "svn+ssh://arcadia.yandex.ru/arc/${REPO_BRANCH}/arcadia/${arc_path}" \
    #     repo

    mkdir -p arcadia store
    arc mount --allow-other --mount arcadia --store store
    cd arcadia
    if [ x"$REPO_BRANCH" != x"trunk" ]; then
        arc fetch "$REPO_BRANCH"
        arc checkout -b somebranch "$REPO_BRANCH"
    fi
    cd "$arc_path"
else
    git clone \
        --recurse-submodules \
        --depth 1 \
        --single-branch \
        --branch "$REPO_BRANCH" \
        "$REPO_URL" \
        repo
    cd repo
fi

# compose and hacks
# TODO: put docker-compose into the base container.
python3.7 -m venv .venv
. ./.venv/bin/activate

if [ "$EXTRADBG" ]; then
    python3.7 -V >&2
    pip -V >&2
    df -h >&2
fi

# ...
if ! which docker-compose; then
    pip install -U pip setuptools
    pip install -U docker-compose
fi

if [ "$EXTRADBG" ]; then
    (
        while true; do
            echo " ======= status:" >&2
            df -h >&2
            free -m >&2
            ps aux >&2
            sleep 60
        done
    ) &
fi

# The main point:
$CMD
