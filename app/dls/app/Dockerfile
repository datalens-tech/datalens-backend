FROM registry.yandex.net/statinfra/base/bi_mess:latest

ENV PYTHONUNBUFFERED 1

RUN mkdir -p /code

COPY etc /etc
COPY docker/gunicorn_logging.ini /code/gunicorn_logging.ini
COPY /bidls_app /code/app/

# REMINDER: Logrotate fixes: if any logrotate configs change is done in here,
# apply the commands from the base mess.

RUN /code/app/bidls_app subcommands_to_bin

EXPOSE 38080
