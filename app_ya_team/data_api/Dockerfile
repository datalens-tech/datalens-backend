#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

ENV APP_DIR="app_ya_team/data_api"

# Runit
COPY /etc /etc

# Gunicorn
ENV GUNICORN_LISTEN_PORT 80
ENV GUNICORN_LOG_CONFIG /src/$APP_DIR/gunicorn_logging.ini
ENV GUNICORN_CONFIG /src/$APP_DIR/gunicorn_config.py
COPY /docker/gunicorn_logging.ini ${GUNICORN_LOG_CONFIG}
COPY /docker/gunicorn_config.py ${GUNICORN_CONFIG}

# CompEng
COPY docker/postgresql_in_build.sh /tmp
RUN /tmp/postgresql_in_build.sh
# # aiopg:
# ENV BI_COMPENG_PG_URL=postgresql://postgres@:5432/postgres
# # asyncpg:
ENV BI_COMPENG_PG_URL=postgresql://postgres@%2Fvar%2Frun%2Fpostgresql/postgres

# Libs & metapkg
COPY --from=bake_ctx_src_lib / /src

# Sources copy
RUN mkdir -p /src/$APP_DIR
COPY /pyproject.toml /src/$APP_DIR/pyproject.toml
COPY /data_api /src/$APP_DIR/data_api
COPY /README.md /src/$APP_DIR/README.md

# Installation
WORKDIR /src/ops/ci
RUN poetry export --only app_ya_team_data_api --without-hashes --format=requirements.txt > requirements.txt
RUN pip install -r requirements.txt
RUN pip install --no-deps --ignore-installed -r docker_image_base_ci/requirements_conflicting.txt
RUN pip install --index-url https://pypi.yandex-team.ru/simple --no-deps --ignore-installed --force-reinstall "$(cat requirements.txt | grep certifi-yandex)"

# Entrypoint
EXPOSE ${GUNICORN_LISTEN_PORT}

WORKDIR /src/$APP_DIR
