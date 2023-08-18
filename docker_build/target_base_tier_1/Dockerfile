#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1

RUN mkdir -p /install
COPY --from=docker_image_base_ci /install_py_310.sh /install/install_py_310.sh

# Installing Python & tooling
RUN bash /install/install_py_310.sh
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
RUN python3 -m pip install -i https://pypi.yandex-team.ru/simple -U pip
# TODO FIX: try to install j2cli to dedicated venv & make symlink in /usr/local/bin
RUN python3 -m pip install -i https://pypi.yandex-team.ru/simple poetry==1.5.0 j2cli==0.3.10
