#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img as build

ARG PACKAGE_NAME
ARG DOMAIN_NAME
ARG PATH_MASK
ARG LOCALES_DIR=generated_locales

WORKDIR /src
COPY --from=src . .

RUN find $PACKAGE_NAME | grep ".*\.\(py\)\|\(jinja\)" | grep $PATH_MASK > POTFILES
RUN xgettext -d $DOMAIN_NAME --keyword=Translatable --files-from=POTFILES --from-code=utf-8
RUN for locale in $(ls $PACKAGE_NAME/locales/) ; do \
    mkdir -p $LOCALES_DIR/$locale/LC_MESSAGES; \
    msgmerge $PACKAGE_NAME/locales/$locale/LC_MESSAGES/$DOMAIN_NAME.po $DOMAIN_NAME.po > $LOCALES_DIR/$locale/LC_MESSAGES/$DOMAIN_NAME.po; \
    done


# Resulting artifact
FROM scratch

COPY --from=build /src/generated_locales /locales
