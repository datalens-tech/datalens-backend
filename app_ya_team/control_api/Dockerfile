#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

ENV APP_DIR="app_ya_team/control_api"

# Runit
COPY /etc /etc

# uWSGI
ENV HTTP_BIND_PORT=80
ENV UWSGI_INI_FOR_APP=/src/$APP_DIR/uwsgi/uwsgi-control-api.ini
ENV UWSGI_INI_FOR_RQE=/src/$APP_DIR/uwsgi/uwsgi-query-executor.ini
COPY /uwsgi/uwsgi-control-api.ini ${UWSGI_INI_FOR_APP}
COPY /uwsgi/uwsgi-query-executor.ini ${UWSGI_INI_FOR_RQE}

# Libs & metapkg
COPY --from=bake_ctx_src_lib / /src

# Sources copy
RUN mkdir -p /src/$APP_DIR
COPY /pyproject.toml /src/$APP_DIR/pyproject.toml
COPY /control_api /src/$APP_DIR/control_api
COPY /README.md /src/$APP_DIR/README.md

# Installation
WORKDIR /src/ops/ci
RUN poetry export --only app_ya_team_control_api --without-hashes --format=requirements.txt > requirements.txt
RUN pip install -r requirements.txt
RUN pip install --no-deps --ignore-installed -r docker_image_base_ci/requirements_conflicting.txt
RUN pip install --index-url https://pypi.yandex-team.ru/simple --no-deps --ignore-installed --force-reinstall "$(cat requirements.txt | grep certifi-yandex)"

# Entrypoint
EXPOSE ${HTTP_BIND_PORT}

WORKDIR /src/$APP_DIR

ENTRYPOINT ["/etc/service/control_api/run"]
