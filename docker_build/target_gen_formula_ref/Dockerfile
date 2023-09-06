#syntax=docker/dockerfile:1.4
FROM base_img as build

ARG LOCALE_NAME
ARG OUTPUT_DIR
ARG CONFIG_VERSION
ARG DOC_DIR=generated_docs

WORKDIR /src

RUN mkdir -p generated_docs
RUN . /venv/bin/activate && \
    bi-formula-ref-doc generate --locale $LOCALE_NAME $DOC_DIR --config-version $CONFIG_VERSION


# Resulting artifact
FROM scratch

COPY --from=build /src/generated_docs /
