#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

ENV APP_DIR="app/dl_control_api"

RUN mkdir -p /code
COPY /etc /etc
COPY /uwsgi /code/app/uwsgi

# Libs & metapkg
COPY --from=bake_ctx_src_lib / /src
COPY --from=bake_ctx_metapkg / /src

# Sources copy
RUN mkdir -p /src/$APP_DIR
COPY /pyproject.toml /src/$APP_DIR/pyproject.toml
COPY /dl_control_api /src/$APP_DIR/dl_control_api
COPY /README.md /src/$APP_DIR/README.md

# Installation
WORKDIR /src/metapkg
RUN poetry export --only app_dl_os_control_api --without-hashes --format=requirements.txt > requirements.txt
RUN pip install -r requirements.txt

EXPOSE 8080

ENTRYPOINT ["/etc/service/bi_api/run"]
