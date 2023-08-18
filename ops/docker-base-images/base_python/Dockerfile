FROM python:3.7.3-stretch

ENV DEBIAN_FRONTEND noninteractive
ENV container docker
ENV LC_ALL "C.UTF-8"
ENV LANG "C.UTF-8"

COPY yandex.asc /etc/apt/trusted.gpg.d/

RUN \
    echo 'deb http://common.dist.yandex.ru/common stable/all/' >> /etc/apt/sources.list.d/yandex.list \
    && \
    echo 'deb http://common.dist.yandex.ru/common stable/amd64/' >> /etc/apt/sources.list.d/yandex.list \
    && \
    apt-get -y update \
    && \
    apt-get -y upgrade \
    && \
    apt-get install -y \
        telnet tcpdump rsync iputils-ping bind9-host yandex-internal-root-ca psmisc \
        libssl-dev libcurl4-openssl-dev default-libmysqlclient-dev iptables \
        libaio1 unzip freetds-dev tdsodbc unixodbc unixodbc-dev git less vim sudo libtcmalloc-minimal4 \
    && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


RUN \
    pip3 install -U pip==20.1.1 \
    && \
    if [ -e /root/.cache ]; then rm -rf /root/.cache; fi

# # Neets to be done after `certifi` as it replases its file:
# RUN pip install --index-url https://pypi.yandex-team.ru/simple --no-deps --upgrade --force-reinstall certifi-yandex
