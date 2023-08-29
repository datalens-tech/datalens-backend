ARG BASE_IMG=registry.yandex.net/statinfra/base/bi_mess:latest
FROM $BASE_IMG

COPY install_py_310.sh /install_py_310.sh
RUN bash /install_py_310.sh

COPY 100-docker.sh /100-docker.sh
RUN bash /100-docker.sh

COPY requirements_external.txt /requirements_external.txt
COPY requirements_conflicting.txt /requirements_conflicting.txt
RUN mkdir /venv \
    && python -m venv /venv \
    && . /venv/bin/activate

RUN . /venv/bin/activate \
    && pip install -r /requirements_external.txt  # only third party deps here


# WORKAROUND DUE TO DEP CONFILICTS in the bigquery
# kchupin: Эти штуки не указаны в зависимостях наших пакетов, но импортятся.
#   Это сделано из-за конфликтов в аркадийных версиях (например grpcio требуется отличный от Аркадийного)
RUN . /venv/bin/activate \
    && pip install --no-deps --ignore-installed -r /requirements_conflicting.txt
# ENDOF WORKAROUND

# ensure that certifi yandex installed last
RUN . /venv/bin/activate \
    pip install  --no-deps --ignore-installed --force-reinstall $(cat /requirements_external.txt | grep certifi-yandex)


COPY remote-python-entrypoint.sh /entrypoint.sh


ENTRYPOINT [ "/entrypoint.sh" ]


EXPOSE 51042
