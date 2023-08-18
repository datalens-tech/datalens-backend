FROM registry.yandex.net/statinfra/base/ubuntu_rootless:latest

RUN mkdir -p /code

COPY etc /etc
COPY /gunicorn_logging.ini /code/gunicorn_logging.ini
COPY /bi_file_uploader_worker /code/app/

RUN /code/app/bi_file_uploader_worker subcommands_to_bin
