FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update &&\
    apt install -y curl git python3 python3-pip &&\
    apt clean &&\
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip install --no-cache-dir bump2version==1.0.1
