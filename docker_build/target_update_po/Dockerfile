#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img as build

ARG PACKAGE_NAME
ARG LOCALES_DIR=generated_locales

WORKDIR /src
COPY --from=src . .

RUN find $PACKAGE_NAME -name "*.py" > POTFILES
RUN xgettext -d $PACKAGE_NAME --keyword=Translatable --files-from=POTFILES --from-code=utf-8
RUN for locale in $(ls $PACKAGE_NAME/locales/) ; do \
    mkdir -p $LOCALES_DIR/$locale/LC_MESSAGES; \
    msgmerge $PACKAGE_NAME/locales/$locale/LC_MESSAGES/$PACKAGE_NAME.po $PACKAGE_NAME.po > $LOCALES_DIR/$locale/LC_MESSAGES/$PACKAGE_NAME.po; \
    done


# Resulting artifact
FROM scratch

COPY --from=build /src/generated_locales /locales
