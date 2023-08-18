FROM registry.yandex.net/statinfra/base/ubuntu_rootless:latest

RUN mkdir -p /code/app

ENV GUNICORN_LOG_CONFIG /code/gunicorn_logging.ini
ENV GUNICORN_CONFIG /code/gunicorn_config.py
COPY docker/gunicorn_logging.ini ${GUNICORN_LOG_CONFIG}
COPY docker/gunicorn_config.py ${GUNICORN_CONFIG}

COPY etc /etc

# REMINDER: Logrotate fixes: if any logrotate configs change is done in here,
# apply the commands from the base mess.

COPY /biapi_app /code/app/

RUN /code/app/biapi_app subcommands_to_bin

EXPOSE 8080

ENTRYPOINT ["/etc/service/bi_api/run"]
