FROM registry.yandex.net/statinfra/base/ubuntu_rootless:latest

RUN mkdir -p /code/app

COPY etc /etc
COPY uwsgi /code/app/uwsgi
COPY /bi_rqe_ext_sync /code/app/

ENTRYPOINT ["/etc/service/bi_rqe_ext_sync/run"]
