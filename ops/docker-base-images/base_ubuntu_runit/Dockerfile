# https://github.com/phusion/baseimage-docker
# to be updated explicitly

# ubuntu 20.04:
# FROM phusion/baseimage:focal-1.1.0
# FROM phusion/baseimage@sha256:12aeeecfff1a6af0b3b65291aba13f7641d1f3fd6d6c4098507702f33c03c096
ARG BASE_IMG=registry.yandex.net/statinfra/base/phusion:focal-1.1.0

FROM $BASE_IMG

ENV DEBIAN_FRONTEND noninteractive
ENV container docker
RUN update-locale LANG=C.UTF-8
ENV LC_ALL "C.UTF-8"
ENV LANG "C.UTF-8"


# To enable sshd:
# run rm /etc/service/sshd/down


# 'Application' directories (heavily suggested paths).
RUN mkdir -p /var/log/app /opt/build/app /opt/app

COPY trusted.gpg.d/ /etc/apt/trusted.gpg.d/

RUN for repo in focal focal-security focal-updates focal-backports; do echo "deb http://mirror.yandex.ru/ubuntu ${repo} main restricted universe multiverse"; done > /etc/apt/sources.list \
    && echo 'deb http://common.dist.yandex.ru/common stable/all/' >> /etc/apt/sources.list.d/yandex.list \
    && echo 'deb http://common.dist.yandex.ru/common stable/amd64/' >> /etc/apt/sources.list.d/yandex.list \
    && apt-get update \
    && apt-get -y upgrade -o Dpkg::Options::="--force-confold" \
    && apt-get install -y --fix-missing \
        bind9-host coreutils curl wget devscripts dnsutils git htop iproute2 iputils-ping joe \
        jq less netcat-openbsd net-tools psmisc rsync screen strace sudo tcpdump \
        telnet tzdata unzip uuid vim \
        gosu \
        libssl-dev \
        build-essential python3.9 python3.9-dev python3-pip python3.9-venv \
        yandex-internal-root-ca \
    && rm -f /etc/ssh/ssh_host_*_key \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip3 install -i https://pypi.yandex-team.ru/simple -U pip==20.1.1 setuptools==53.1.0 wheel==0.36.2 \
    && if [ -e /root/.cache ]; then rm -rf /root/.cache; fi

RUN ln -sf python3.9 /usr/bin/python && ln -sf python3.9 /usr/bin/python3

# # Needs to be done after `certifi` as it replases its file:
# RUN pip install --index-url https://pypi.yandex-team.ru/simple --no-deps --upgrade --force-reinstall certifi-yandex


# cmd ["/sbin/my_init"]
