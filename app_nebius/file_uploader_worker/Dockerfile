#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

ARG APP_NAME
ENV APP_NAME=$APP_NAME

# Libs & metapkg
COPY --from=bake_ctx_src_lib / /src

# Sources copy
RUN mkdir -p /src/app_nebius

COPY /$APP_NAME /src/app_nebius/$APP_NAME/$APP_NAME
COPY /pyproject.toml /src/app_nebius/$APP_NAME/pyproject.toml
COPY /README.md /src/app_nebius/$APP_NAME/README.md

# Installation
WORKDIR /src/ops/ci
RUN poetry export --only nebius_$APP_NAME --without-hashes --format requirements.txt --output requirements.txt
RUN pip install -r requirements.txt
RUN pip install --no-deps --ignore-installed -r docker_image_base_ci/requirements_conflicting.txt

EXPOSE 8080
WORKDIR /src/app_nebius/$APP_NAME

ENTRYPOINT file-uploader-worker
