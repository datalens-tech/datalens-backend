#syntax=docker/dockerfile:1.4
FROM ubuntu:24.04

# Installing deb tools
ENV DEBIAN_FRONTEND=noninteractive \
    container=docker

COPY trusted.gpg.d/ /etc/apt/trusted.gpg.d/
COPY install_py_310.sh /tmp/install_py_310.sh

RUN apt-get update \
    && apt-get -y upgrade -o Dpkg::Options::="--force-confold" \
    && apt-get install -y --fix-missing \
        # For some python libraries
        libpcre3 \
        # For MSSQL
        libct4 libsybdb5 tdsodbc unixodbc libtcmalloc-minimal4 \
        # For Postgres
        libpq5 \
    && rm -f /etc/ssh/ssh_host_*_key \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Configure libraries for MSSQL
RUN cat /usr/share/tdsodbc/odbcinst.ini >> /etc/odbcinst.ini
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu/odbc

# install python 3.10
RUN bash /tmp/install_py_310.sh

RUN curl "https://storage.yandexcloud.net/cloud-certs/CA.pem" > /usr/local/share/ca-certificates/yandex-cloud-ca.crt
RUN update-ca-certificates

ENV REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"
