# ubuntu 20.04:
# FROM ubuntu:focal-20230605
FROM registry.yandex.net/statinfra/base/ubuntu:focal-20230605

ENV DEBIAN_FRONTEND noninteractive
ENV container docker

COPY trusted.gpg.d/ /etc/apt/trusted.gpg.d/

RUN for repo in focal focal-security focal-updates focal-backports; do echo "deb http://mirror.yandex.ru/ubuntu ${repo} main restricted universe multiverse"; done > /etc/apt/sources.list \
    && echo 'deb http://common.dist.yandex.ru/common stable/all/' >> /etc/apt/sources.list.d/yandex.list \
    && echo 'deb http://common.dist.yandex.ru/common stable/amd64/' >> /etc/apt/sources.list.d/yandex.list \
    && apt-get update \
    && apt-get -y upgrade -o Dpkg::Options::="--force-confold" \
    && apt-get install -y --fix-missing \
        curl dnsutils htop iputils-ping jq less telnet vim \
        yandex-internal-root-ca \
    && rm -f /etc/ssh/ssh_host_*_key \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV REQUESTS_CA_BUNDLE "/etc/ssl/certs/ca-certificates.crt"
