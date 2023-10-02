#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

ENV APP_DIR="app_ya_team/file_uploader_api"

# Runit
COPY /etc /etc

# Gunicorn
ENV GUNICORN_LISTEN_PORT=80
ENV GUNICORN_LOG_CONFIG=/src/$APP_DIR/gunicorn_logging.ini
COPY /docker/gunicorn_logging.ini ${GUNICORN_LOG_CONFIG}

# Libs & metapkg
COPY --from=bake_ctx_src_lib / /src

# Sources copy
RUN mkdir -p /src/$APP_DIR
COPY /pyproject.toml /src/$APP_DIR/pyproject.toml
COPY /file_uploader_api /src/$APP_DIR/file_uploader_api
COPY /README.md /src/$APP_DIR/README.md

# Installation
WORKDIR /src/ops/ci
RUN poetry export --only app_ya_team_file_uploader_api --without-hashes --format=requirements.txt > requirements.txt
RUN pip install -r requirements.txt

# Entrypoint
EXPOSE ${GUNICORN_LISTEN_PORT}

WORKDIR /src/$APP_DIR
