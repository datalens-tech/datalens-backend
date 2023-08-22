#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img as build

ARG PACKAGE_NAME
ARG DOMAIN_NAME
ARG LOCALES_DIR=generated_locales

WORKDIR /src
COPY --from=src . .

RUN for locale in $(ls $PACKAGE_NAME/locales/) ; do \
    mkdir -p $LOCALES_DIR/$locale/LC_MESSAGES; \
	msgfmt -o $LOCALES_DIR/$locale/LC_MESSAGES/$DOMAIN_NAME.mo $PACKAGE_NAME/locales/$locale/LC_MESSAGES/$DOMAIN_NAME.po ; \
	done

# Resulting artifact
FROM scratch

COPY --from=build /src/generated_locales /locales
