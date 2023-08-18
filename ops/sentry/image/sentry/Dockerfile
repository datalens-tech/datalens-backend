FROM python:2.7.12-slim

ARG SENTRY_VERSION

ENV TZ='Europe/Moscow' \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    C_FORCE_ROOT=1 \
    SENTRY_VERSION=${SENTRY_VERSION} \
    SENTRY_CONF=/etc/sentry \
    SENTRY_FILESTORE_DIR=/var/lib/sentry/files \
    DEBIAN_FRONTEND=noninteractive

RUN echo $TZ > /etc/timezone

RUN groupadd -r sentry && \
    useradd -r -m -g sentry sentry

RUN mkdir -p $SENTRY_CONF && \
    mkdir -p $SENTRY_FILESTORE_DIR

COPY deps/debian.txt deps/debian.txt

# Исправляем source-list'ы в debian:jessie
RUN printf "deb http://archive.debian.org/debian/ jessie main\ndeb-src http://archive.debian.org/debian/ jessie main\ndeb http://security.debian.org jessie/updates main\ndeb-src http://security.debian.org jessie/updates main" > /etc/apt/sources.list
RUN apt-get update && \
    cat deps/debian.txt | xargs apt-get install -y --force-yes --no-install-recommends && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ADD http://crls.yandex.net/YandexInternalRootCA.crt /etc/ssl/certs/YandexInternalRootCA.pem
ADD https://crls.yandex.net/allCAs.pem /etc/ssl/certs/allCAs.pem

COPY deps/python.txt deps/python.txt

RUN envsubst < deps/python.txt | xargs pip install --extra-index-url https://pypi.yandex-team.ru/simple/
# hack: django_yauth uses a descriptor; problem is, a descriptor can be overridden by a value on the object. So, make sure there's no such value.
RUN sed -i~ -r 's/^( +)(request\.__class__\.user = DjangoUserDescriptor\(\))/\1\2\n\1request.__dict__.pop("user", None)/' /usr/local/lib/python2.7/site-packages/django_yauth/middleware.py

ADD http://crls.yandex.net/YandexInternalRootCA.crt /etc/ssl/certs/ca-certificates.crt

COPY /etc /etc
COPY /usr /usr

CMD /usr/local/bin/sentry.sh
