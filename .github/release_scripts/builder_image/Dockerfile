FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update &&\
    apt install -y curl git python3 python3-pip &&\
    apt clean &&\
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip install --no-cache-dir bump2version==1.0.1

RUN rm /etc/apt/sources.list &&\
    rm -rf /etc/apt/sources.list.d/* && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg &&\
    chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg &&\
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | tee /etc/apt/sources.list.d/github-cli.list > /dev/null &&\
    apt update -qq &&\
    apt install gh -y &&\
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
