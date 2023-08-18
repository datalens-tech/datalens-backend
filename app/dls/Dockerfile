FROM registry.yandex.net/statinfra/base/bi_mess:latest
# ^ https://bb.yandex-team.ru/projects/STATBOX/repos/docker/browse/bi_base_mess/Dockerfile

ENV PYTHONUNBUFFERED 1

# docker cache optimization
RUN mkdir /build
WORKDIR /build
COPY setup.py requirements.txt /build/

# Note: `certifi-yandex` overwrites files from `certifi`, so have to make sure
# it is installed after, not before.
# Note: aiopg is pending an upstream merge; see https://st.yandex-team.ru/BI-910
RUN \
    pip install --upgrade pip==20.1.1 \
    && \
    pip install \
        -r requirements.txt \
        -e '.[srv,web,tasks]' \
    && \
    pip install --no-deps --upgrade --force-reinstall git+https://github.com/r-dmv/aiopg.git@9b8e5ae09fa69994e20849557a11a2747b3e1256 \
    && \
    pip install --no-deps --upgrade --force-reinstall -i https://pypi.yandex-team.ru/simple certifi-yandex \
    && \
    if [ -e /root/.cache ]; then rm -rf /root/.cache; fi

COPY ./etc/ /etc/
COPY . /code/
COPY docker/gunicorn_logging.ini /code/gunicorn_logging.ini
WORKDIR /code

RUN \
    pip install \
        --no-deps --force-reinstall --upgrade \
        -e '.[srv,web,tasks]' \
    && \
    if [ -e /root/.cache ]; then rm -rf /root/.cache; fi


EXPOSE 38080
