FROM registry.yandex.net/statinfra/base/ubuntu_rootless:latest

RUN mkdir -p /code

COPY etc /etc
COPY /gunicorn_logging.ini /code/gunicorn_logging.ini
COPY /bi_file_uploader_app /code/app/

RUN /code/app/bi_file_uploader_app subcommands_to_bin

EXPOSE 8080

ENTRYPOINT ["/etc/service/bi_file_uploader/run"]
