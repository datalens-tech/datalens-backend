#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

ENV APP_DIR="app_ya_team/file_uploader_worker"

# Runit
COPY /etc /etc

# Libs & metapkg
COPY --from=bake_ctx_src_lib / /src

# Sources copy
RUN mkdir -p /src/$APP_DIR
COPY /pyproject.toml /src/$APP_DIR/pyproject.toml
COPY /file_uploader_worker /src/$APP_DIR/file_uploader_worker
COPY /README.md /src/$APP_DIR/README.md

# Installation
WORKDIR /src/ops/ci
RUN poetry export --only app_ya_team_file_uploader_worker --without-hashes --format=requirements.txt > requirements.txt
RUN pip install -r requirements.txt

# Entrypoint
WORKDIR /src/$APP_DIR
