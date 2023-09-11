#syntax=docker/dockerfile:1.4
FROM base_img as build

ARG OUTPUT_DIR
ARG CONFIG_VERSION
ARG DOC_DIR=generated_docs

WORKDIR /src

RUN mkdir -p generated_docs
RUN . /venv/bin/activate && \
    for locale in $(bi-formula-ref-doc locales --config-version $CONFIG_VERSION) ; do \
    bi-formula-ref-doc generate --config-version $CONFIG_VERSION --locale $locale $DOC_DIR/$locale ; \
    done


# Resulting artifact
FROM scratch

COPY --from=build /src/generated_docs /
