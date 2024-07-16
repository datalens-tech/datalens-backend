#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

ENV APP_DIR="app/dl_file_uploader_api"

RUN mkdir -p /code
COPY /etc /etc

ENV GUNICORN_LOG_CONFIG /code/gunicorn_logging.ini
ENV GUNICORN_CONFIG /code/gunicorn_config.py
COPY docker/gunicorn_logging.ini ${GUNICORN_LOG_CONFIG}
COPY docker/gunicorn_config.py ${GUNICORN_CONFIG}

# Libs & metapkg
COPY --from=bake_ctx_src_lib / /src
COPY --from=bake_ctx_metapkg / /src

# Sources copy
RUN mkdir -p /src/$APP_DIR
COPY /pyproject.toml /src/$APP_DIR/pyproject.toml
COPY /dl_file_uploader_api /src/$APP_DIR/dl_file_uploader_api
COPY /README.md /src/$APP_DIR/README.md

# Installation
WORKDIR /src/metapkg
RUN poetry export --only app_dl_os_file_uploader_api --without-hashes --format=requirements.txt > requirements.txt
RUN pip install -r requirements.txt

# Configs
ENV CONFIG_PATH="/etc/backend-configs/file_api.yaml"
COPY --from=bake_ctx_app_configs /app_configs/file_api.yaml "$CONFIG_PATH"

# Setting up the runtime user
ARG USER=app
ARG GID=1000
ARG UID=1000
RUN groupadd -r -g ${GID} ${USER} && \
    useradd -mr -g ${USER} -u ${UID} -s /bin/bash ${USER}
USER ${USER}

EXPOSE 8080

ENTRYPOINT ["/etc/service/dl_file_uploader_api/run"]
