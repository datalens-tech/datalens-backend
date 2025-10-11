#!/usr/bin/env bash

set -exu

echo 'deb http://ppa.launchpad.net/deadsnakes/ppa/ubuntu noble main' > /etc/apt/sources.list.d/deadsnakes.list

apt-get update

apt-get install --yes \
  curl \
  gnupg \
  perl \
  gcc \
  make

# TODO remove perl and gcc – needed for installation only
# TODO move gnupg somewhere else
# TODO curl is not needed here also

OPENSSL_PREFIX_DIR="/usr/local/openssl"

curl -fsSL --output openssl-3.5.4.tar.gz https://github.com/openssl/openssl/releases/download/openssl-3.5.4/openssl-3.5.4.tar.gz \
  && curl -fsSL --output openssl-3.5.4.tar.gz.asc https://github.com/openssl/openssl/releases/download/openssl-3.5.4/openssl-3.5.4.tar.gz.asc \
  && export GNUPGHOME="$(mktemp -d)" \
  && (for i in 1 2 3 4 5; do gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys "216094DFD0CB81EF" && break; sleep 3; done) \
  && gpg --batch --verify openssl-3.5.4.tar.gz.asc openssl-3.5.4.tar.gz \
  && { command -v gpgconf > /dev/null && gpgconf --kill all || :; } \
  && rm --force --recursive "$GNUPGHOME" \
  && mkdir --parents /usr/src/openssl \
  && tar -xzC /usr/src/openssl --strip-components=1 -f openssl-3.5.4.tar.gz \
  && rm openssl-3.5.4.tar.gz openssl-3.5.4.tar.gz.asc \
  && cd /usr/src/openssl \
  && ./config --prefix="$OPENSSL_PREFIX_DIR" --openssldir="$OPENSSL_PREFIX_DIR" --libdir=lib '-Wl,-rpath,${OPENSSL_PREFIX_DIR}/lib' \
  && make \
  && make install \
  && rm --force --recursive /usr/src/openssl

echo 'export PATH=/usr/local/openssl:$PATH' >> /etc/profile.d/openssl.sh

. /etc/profile.d/openssl.sh

apt-get install -y python3.10 python3.10-dev python3-pip python3.10-venv || sleep "$y" ;

ln -sf python3.10 /usr/bin/python && ln -sf python3.10 /usr/bin/python3
