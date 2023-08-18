#syntax=docker/dockerfile:1.4
FROM python:3.10-bullseye AS build

ARG GRPCIO_VER=1.45.0rc1
ARG PB_VER=3.20.3
ARG STUBS_DIR=/gen

RUN apt-get update \
    && apt-get install -y protobuf-compiler \
    && apt-get clean

RUN pip3 install -i https://pypi.yandex-team.ru/simple \
    grpcio-tools \
    grpcio==$GRPCIO_VER \
    grpcio-reflection==$GRPCIO_VER \
    protobuf==$PB_VER \
    && if [ -e /root/.cache ]; then rm -rf /root/.cache; fi

ENV PROTO_SRC=/src
COPY --from=proto_all / ${PROTO_SRC}

RUN mkdir $STUBS_DIR

COPY generate_proto_stubs.py /generate_proto_stubs.py

ARG PB_GEN_TARGET

RUN python3 /generate_proto_stubs.py --proto_path=${PROTO_SRC} --stubs_dir=${STUBS_DIR} "${PB_GEN_TARGET}"

# Resulting artifact
FROM scratch

COPY --from=build /gen /
