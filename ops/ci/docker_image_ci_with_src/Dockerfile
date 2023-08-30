ARG BASE_CI_IMAGE=datalens_base_ci:latest
FROM $BASE_CI_IMAGE


COPY --from=src_all / /data/
COPY --from=src_terrarium / /data/mainrepo/

RUN . /venv/bin/activate \
    && pip install -e /data/mainrepo/terrarium/bi_ci

COPY local_deps_install.sh /local_deps_install.sh
RUN bash /local_deps_install.sh

RUN . /venv/bin/activate \
    && python -m pip install --no-deps /data/mainrepo/lib/bi_connector_bigquery

# ensure that certifi yandex installed last
RUN . /venv/bin/activate \
    pip install  --no-deps --ignore-installed --force-reinstall $(cat /requirements_external.txt | grep certifi-yandex)

RUN . /venv/bin/activate && python /data/ops/ci/cr_fixer.py
