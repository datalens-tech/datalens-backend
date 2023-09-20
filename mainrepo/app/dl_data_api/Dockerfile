#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

ENV APP_DIR="app/dl_data_api"

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
COPY /dl_data_api /src/$APP_DIR/dl_data_api
COPY /README.md /src/$APP_DIR/README.md

# Installation
WORKDIR /src/metapkg
RUN poetry export --only app_dl_os_data_api --without-hashes --format=requirements.txt > requirements.txt
RUN pip install -r requirements.txt

EXPOSE 8080

ENTRYPOINT ["/etc/service/bi_api/run"]
