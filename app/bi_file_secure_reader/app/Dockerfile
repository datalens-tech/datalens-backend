# ubuntu 20.04:
# FROM ubuntu:focal-20230126
# FROM ubuntu@sha256:4a45212e9518f35983a976eead0de5eecc555a2f047134e9dd2cfc589076a00d
FROM registry.yandex.net/statinfra/base/ubuntu:focal-20230126

RUN mkdir -p /code/app

COPY etc /etc
COPY /bi_file_secure_reader_app /code/app/

RUN /code/app/bi_file_secure_reader_app subcommands_to_bin

ENTRYPOINT ["/etc/service/bi_file_secure_reader/run"]
