#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img as build

ARG PACKAGE_NAME
ARG DOMAIN_NAME
ARG LOCALES_DIR=generated_locales

WORKDIR /src
COPY --from=src . .
COPY --from=src_terrarium . .

# Install repmanager to access metadata in pyproject.toml
RUN ls -l terrarium
RUN pip install terrarium/dl_repmanager

# For each domain -> for each locale -> generate
RUN for domain_pair in $(dl-package . list-i18n-domains) ; do \
    # Read the <domain_name=scan_path> pair into vars
    export DOMAIN_NAME=$(echo $domain_pair | cut -d "=" -f 1) ; \
    # Do the rest
    for locale in $(ls $PACKAGE_NAME/locales/) ; do \
    mkdir -p $LOCALES_DIR/$locale/LC_MESSAGES ; \
	msgfmt -o $LOCALES_DIR/$locale/LC_MESSAGES/$DOMAIN_NAME.mo $PACKAGE_NAME/locales/$locale/LC_MESSAGES/$DOMAIN_NAME.po ; \
    done; \
    done


# Resulting artifact
FROM scratch

COPY --from=build /src/generated_locales /locales
