#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img AS build

ENV APP_DIR="app/dl_control_api"

# Install global deps
COPY --from=bake_ctx_metapkg / /src

RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH" \
    VIRTUAL_ENV="/venv"

ENV PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VIRTUALENVS_CREATE=0 \
    POETRY_CACHE_DIR=/tmp/poetry_cache \
    SETUPTOOLS_USE_DISTUTILS=stdlib

WORKDIR /src/metapkg
RUN --mount=type=cache,target=$POETRY_CACHE_DIR \
    poetry install --only app_dl_os_control_api --no-root --no-directory

# Install source deps
COPY --from=bake_ctx_src_lib / /src
RUN mkdir -p /src/$APP_DIR
COPY /pyproject.toml /src/$APP_DIR/pyproject.toml
COPY /dl_control_api /src/$APP_DIR/dl_control_api
COPY /README.md /src/$APP_DIR/README.md

RUN poetry export --only app_dl_os_control_api --without-hashes --format=requirements.txt > requirements.txt && \
    pip install -r requirements.txt && \
    find /venv -name "*.pyc" -delete && \
    find /venv -name "__pycache__" -delete

# Runtime configuration
FROM bake_ctx_run AS runtime

ENV PATH="/venv/bin:$PATH" \
    VIRTUAL_ENV="/venv" \
    CONFIG_PATH="/etc/backend-configs/api.yaml" \
    PYTHONUNBUFFERED=1

# Configs
RUN mkdir -p /code
COPY /etc /etc
COPY /uwsgi /code/app/uwsgi
COPY --from=bake_ctx_app_configs /app_configs/api.yaml "$CONFIG_PATH"

COPY --from=build /venv /venv

# Setting up the runtime user
ARG USER=app
ARG GID=1000
ARG UID=1000
RUN userdel -r ubuntu && \
    groupadd -r -g ${GID} ${USER} && \
    useradd -mr -g ${USER} -u ${UID} -s /bin/bash ${USER}
USER ${USER}

EXPOSE 8080

ENTRYPOINT ["/etc/service/dl_api/run"]
