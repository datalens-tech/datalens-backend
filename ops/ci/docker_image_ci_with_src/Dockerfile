ARG BASE_CI_IMAGE=datalens_base_ci:latest
FROM $BASE_CI_IMAGE

# assuming build perforemed in the datalens project root

RUN mkdir -p /data
COPY lib /data/lib
COPY app /data/app
COPY terrarium /data/terrarium
COPY app_yc /data/app_yc
RUN mkdir -p /data/ops

COPY ops/ci/pyproject.toml /data/ops/ci/
COPY ops/ci/poetry.lock /data/ops/ci/
COPY ops/ci/bi_ci_dummy /data/ops/ci/bi_ci_dummy
COPY ops/ci/networks_addon.yml /data/ops/ci/networks_addon.yml
COPY ops/bi_integration_tests /data/ops/bi_integration_tests

RUN mkdir -p /data/tools
RUN echo 2 && ls -laht /data/tools
COPY ops/ci/Makefile /data/ops/ci/

# todo: merge with the main third party requiremets, add tooling to sync versions stubs with main pkg
COPY ops/ci/docker_image_ci_with_src/requirements_types.txt /requirements_types.txt
RUN . /venv/bin/activate \
    && pip install -r requirements_types.txt

RUN . /venv/bin/activate \
    && pip install -e /data/terrarium/bi_ci

COPY ops/ci/docker_image_ci_with_src/local_deps_install.sh /local_deps_install.sh
RUN bash /local_deps_install.sh

RUN . /venv/bin/activate \
    && python -m pip install --no-deps /data/lib/bi_connector_bigquery

# ensure that certifi yandex installed last
RUN . /venv/bin/activate \
    pip install  --no-deps --ignore-installed --force-reinstall $(cat /requirements_external.txt | grep certifi-yandex)

COPY ops/ci/cr_fixer.py /data/ops/ci/
RUN . /venv/bin/activate && python /data/ops/ci/cr_fixer.py
