#syntax=docker/dockerfile:1.4
FROM ubuntu:24.04

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
COPY install_py_312.sh /install/install_py_312.sh

RUN bash /install/install_py_312.sh

# Install poetry into an isolated pipx venv.
# PIPX_HOME/PIPX_BIN_DIR override pipx's per-user defaults so binaries land on
# the system PATH (used in lieu of `pipx install --global`, which requires
# pipx >= 1.5.0; noble ships an older version).
# pyproject-hooks==1.0.0 pinned for poetry 1.7.1 compatibility:
# https://github.com/python-poetry/poetry/issues/9351
# setuptools + wheel injected alongside the pyproject-hooks pin so poetry's
# chef can build sdists for legacy packages (no pyproject.toml, e.g.
# opentracing 2.4.0, threadloop 1.0.2).
ENV PIPX_HOME=/opt/pipx
ENV PIPX_BIN_DIR=/usr/local/bin
RUN pipx install poetry==1.7.1 \
    && pipx inject poetry pyproject-hooks==1.0.0 setuptools wheel

# Tiny Jinja2 CLI replaces archived j2cli (broken on Python 3.12) for the
# `j2 TEMPLATE > OUT` pattern in bi_base_mess service scripts. Uses
# python3-jinja2 (apt) installed by install_py_312.sh, no pip/venv layer.
COPY j2_wrapper.py /usr/local/bin/j2
RUN chmod +x /usr/local/bin/j2 \
    && echo 'hello {{ name }}' > /tmp/_j2_test.j2 \
    && name=world j2 /tmp/_j2_test.j2 | grep -q '^hello world$' \
    && rm /tmp/_j2_test.j2

RUN curl "https://storage.yandexcloud.net/cloud-certs/CA.pem" > /usr/local/share/ca-certificates/yandex-cloud-ca.crt
RUN update-ca-certificates

ENV REQUESTS_CA_BUNDLE "/etc/ssl/certs/ca-certificates.crt"
