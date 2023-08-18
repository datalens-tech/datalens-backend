FROM registry.yandex.net/statinfra/base/bi_mess:latest

# ### py ###
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
RUN pip3 install -i https://pypi.yandex-team.ru/simple -U py-spy==0.3.14 \
    && if [ -e /root/.cache ]; then rm -rf /root/.cache; fi

COPY docker/postgresql_in_build.sh /tmp
RUN /tmp/postgresql_in_build.sh
# # aiopg:
# ENV BI_COMPENG_PG_URL=postgresql://postgres@:5432/postgres
# # asyncpg:
ENV BI_COMPENG_PG_URL=postgresql://postgres@%2Fvar%2Frun%2Fpostgresql/postgres

RUN mkdir -p /code

ENV GUNICORN_LISTEN_PORT 80
ENV GUNICORN_LOG_CONFIG /code/gunicorn_logging.ini
ENV GUNICORN_CONFIG /code/gunicorn_config.py
COPY docker/gunicorn_logging.ini ${GUNICORN_LOG_CONFIG}
COPY docker/gunicorn_config.py ${GUNICORN_CONFIG}

COPY etc /etc

# REMINDER: Logrotate fixes: if any logrotate configs change is done in here,
# apply the commands from the base mess.

COPY /uwsgi /code/app/uwsgi
COPY /biapi_app /code/app/

RUN /code/app/biapi_app subcommands_to_bin

EXPOSE 80
