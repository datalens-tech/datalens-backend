#syntax=docker/dockerfile:1.4
FROM ubuntu:22.04

# Installing deb tools
ENV DEBIAN_FRONTEND noninteractive
ENV container docker

COPY trusted.gpg.d/ /etc/apt/trusted.gpg.d/

RUN apt-get update \
    && apt-get -y upgrade -o Dpkg::Options::="--force-confold" \
    && apt-get install -y --fix-missing \
        curl dnsutils iputils-ping jq less telnet unzip vim \
        # For some python libraries
        libssl-dev libcurl4-openssl-dev libpcre3-dev libpcre3 libyaml-dev libffi-dev \
    && rm -f /etc/ssh/ssh_host_*_key \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Installing Python & tooling
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1

RUN mkdir -p /install
COPY install_py_310.sh /install/install_py_310.sh

RUN bash /install/install_py_310.sh
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
RUN python3 -m pip install -U pip
# TODO FIX: try to install j2cli to dedicated venv & make symlink in /usr/local/bin
RUN python3 -m pip install poetry==1.7.1 j2cli==0.3.10

RUN curl "https://storage.yandexcloud.net/cloud-certs/CA.pem" > /usr/local/share/ca-certificates/yandex-cloud-ca.crt
RUN update-ca-certificates

ENV REQUESTS_CA_BUNDLE "/etc/ssl/certs/ca-certificates.crt"
