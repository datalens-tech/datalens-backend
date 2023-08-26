ARG BASE_CI_IMAGE
FROM $BASE_CI_IMAGE

# todo: merge with the main third party requiremets, add tooling to sync versions stubs with main pkg
COPY ops/ci/docker_image_ci_mypy/requirements_types.txt /requirements_types.txt
RUN . /venv/bin/activate \
    && pip install -r requirements_types.txt
