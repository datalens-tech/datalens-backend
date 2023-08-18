#syntax=docker/dockerfile:1.4
FROM registry.yandex.net/statinfra/base/ubuntu:focal-20230605

# Installing deb tools
ENV DEBIAN_FRONTEND noninteractive
ENV container docker

COPY trusted.gpg.d/ /etc/apt/trusted.gpg.d/

RUN for repo in focal focal-security focal-updates focal-backports; do echo "deb http://mirror.yandex.ru/ubuntu ${repo} main restricted universe multiverse"; done > /etc/apt/sources.list \
    && echo 'deb http://common.dist.yandex.ru/common stable/all/' >> /etc/apt/sources.list.d/yandex.list \
    && echo 'deb http://common.dist.yandex.ru/common stable/amd64/' >> /etc/apt/sources.list.d/yandex.list \
    && apt-get update \
    && apt-get -y upgrade -o Dpkg::Options::="--force-confold" \
    && apt-get install -y --fix-missing \
        curl dnsutils htop iputils-ping jq less telnet unzip vim \
        # For some python libraries
        libssl-dev libcurl4-openssl-dev libpcre3-dev libpcre3 libyaml-dev libffi-dev \
        yandex-internal-root-ca \
    && rm -f /etc/ssh/ssh_host_*_key \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Installing Python & tooling
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1

RUN mkdir -p /install
COPY install_py_310.sh /install/install_py_310.sh

RUN bash /install/install_py_310.sh
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
RUN python3 -m pip install -i https://pypi.yandex-team.ru/simple -U pip
# TODO FIX: try to install j2cli to dedicated venv & make symlink in /usr/local/bin
RUN python3 -m pip install -i https://pypi.yandex-team.ru/simple poetry==1.5.0 j2cli==0.3.10

ENV REQUESTS_CA_BUNDLE "/etc/ssl/certs/ca-certificates.crt"
