#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

ENV APP_DIR="app/dl_file_uploader_worker"

# Runit
COPY /etc /etc

# Libs & metapkg
COPY --from=bake_ctx_src_lib / /src
COPY --from=bake_ctx_metapkg / /src

# Sources copy
RUN mkdir -p /src/$APP_DIR
COPY /pyproject.toml /src/$APP_DIR/pyproject.toml
COPY /dl_file_uploader_worker /src/$APP_DIR/dl_file_uploader_worker
COPY /README.md /src/$APP_DIR/README.md

# Installation
WORKDIR /src/metapkg
RUN poetry export --only app_dl_os_file_uploader_worker --without-hashes --format=requirements.txt > requirements.txt
RUN pip install -r requirements.txt

# Entrypoint
WORKDIR /src/$APP_DIR

# Configs
ENV CONFIG_PATH="/etc/backend-configs/file_worker.yaml"
COPY --from=bake_ctx_app_configs /app_configs/file_worker.yaml "$CONFIG_PATH"

ENTRYPOINT ["/etc/service/file_uploader_worker/run"]

