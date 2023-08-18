FROM registry.yandex.net/statinfra/base/ubuntu_rootless:latest

RUN mkdir -p /code/app

COPY etc /etc
COPY /bi_rqe_ext_async /code/app/

ENTRYPOINT ["/etc/service/bi_rqe_ext_async/run"]
