FROM registry.yandex.net/statinfra/base/bi_mess:latest
# ^ https://bb.yandex-team.ru/projects/STATBOX/repos/docker/browse/bi_base_mess/Dockerfile

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

# Preinstall the requirements (docker caching)
COPY requirements_actual.txt /code/app/requirements.txt
WORKDIR /code/app/
RUN pip3 install -U pip==20.1.1 \
    && pip3 install -r requirements.txt \
    && pip3 install --index-url https://pypi.yandex-team.ru/simple --no-deps --upgrade --force-reinstall certifi-yandex \
    && if [ -e /root/.cache ]; then rm -rf /root/.cache; fi

ENV FLASK_APP "/code/app/bi/app.py"

COPY etc /etc

# REMINDER: Logrotate fixes: if any logrotate configs change is done in here,
# apply the commands from the base mess.

# TODO FIX: Copy only required dirs (or turn .dockerignore into a whitelist)
COPY / /code/app/
RUN for dirname in /code/app/local_libs/*; do pip3 install --no-deps --force-reinstall -e "$dirname"; done \
    && pip3 install --no-deps --force-reinstall -e /code/app

# The following used to fix MSSQL crash (see https://github.com/r9y9/gantts/issues/14)
# ENV LD_PRELOAD "/usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4"

EXPOSE 80
