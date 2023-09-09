ARG BASE_IMG
FROM $BASE_IMG

COPY --from=dl_src_lib / /src/

RUN . /venv/bin/activate && cd /src/metapkg/ && poetry install --no-root --without=dev --with=ci