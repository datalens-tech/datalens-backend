FROM registry.yandex.net/statinfra/base/bi_mess:latest

RUN mkdir -p /code

COPY etc /etc
COPY /gunicorn_logging.ini /code/gunicorn_logging.ini
COPY /bi_testing_db_provision_app /code/app/

# REMINDER: Logrotate fixes: if any logrotate configs change is done in here,
# apply the commands from the base mess.

RUN /code/app/bi_testing_db_provision_app subcommands_to_bin

EXPOSE 80
