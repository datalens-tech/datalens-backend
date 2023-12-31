#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img as build

ARG PACKAGE_NAME
ARG LOCALES_DIR=generated_locales
ARG MSGID_BUGS_ADDRESS

WORKDIR /src
COPY --from=src . .
COPY --from=src_terrarium . .

# Install repmanager to access metadata in pyproject.toml
RUN ls -l terrarium
RUN pip install terrarium/dl_repmanager

# Prepare a config file for babel. It's always the same, no ponit in storing in in packages
RUN echo "[python: **.py]\n[jinja2: **.jinja]" > babel_.ini
RUN pip install Babel pytz jinja2

# For each domain -> for each locale -> generate raw .po, merge it into the existing .po
RUN for domain_pair in $(dl-package --package-path=. list-i18n-domains) ; do \
    # Read the <domain_name=scan_path> pair into vars
    export DOMAIN_NAME=$(echo $domain_pair | cut -d "=" -f 1) ; \
    export SCAN_PATHS="$(echo $domain_pair | cut -d "=" -f 2 | sed 's/;/ /g')" ; \
    # Do the rest (extract raw .po, iterate over locales, merge the .po) \
    # Use `sh -c` here because $SCAN_PATH is a space-separated list that needs to be passes as separate args
    sh -c "pybabel extract --no-location --keyword=Translatable --msgid-bugs-address $MSGID_BUGS_ADDRESS -F babel_.ini -o $DOMAIN_NAME.po $SCAN_PATHS" ; \
    for locale in $(ls $PACKAGE_NAME/locales/) ; do \
    mkdir -p $LOCALES_DIR/$locale/LC_MESSAGES ; \
    msgmerge $PACKAGE_NAME/locales/$locale/LC_MESSAGES/$DOMAIN_NAME.po $DOMAIN_NAME.po > $LOCALES_DIR/$locale/LC_MESSAGES/$DOMAIN_NAME.po; \
    # Now replace the date with a fixed one so that it doesn't get updated every time
    sed -i -r "s/POT-Creation-Date: [0-9]+-[0-9]+-[0-9]+ [0-9]+:[0-9]+\\+[0-9]+/POT-Creation-Date: 2023-09-22 08:16+0000/g" $LOCALES_DIR/$locale/LC_MESSAGES/$DOMAIN_NAME.po; \
    done; \
    done


# Resulting artifact
FROM scratch

COPY --from=build /src/generated_locales /locales
