ARG BASE_IMG=registry.yandex.net/statinfra/base/ubuntu_runit:latest
# ^ ../base_ubuntu_runit
FROM $BASE_IMG

# #######
# BI-relevant base image that contains libraries for a few databases.
# #######

# ### deb ###

COPY trusted.gpg.d/ /etc/apt/trusted.gpg.d/

RUN echo 'deb https://repo.yandex.ru/clickhouse/deb stable main' >> /etc/apt/sources.list.d/clickhouse.list \
    && echo 'deb https://mirror.yandex.ru/mirrors/postgresql focal-pgdg main' > /etc/apt/sources.list.d/pgdg.list \
    && echo 'deb http://dist.yandex.ru/search-focal/ stable/amd64/' >> /etc/apt/sources.list.d/yandex.list \
    && echo 'deb http://dist.yandex.ru/search-focal/ stable/all/' >> /etc/apt/sources.list.d/yandex.list \
    && apt-get update \
    && apt-get -y upgrade -o Dpkg::Options::="--force-confold" \
    && apt-get remove -y software-properties-common python3-gi && apt-get autoremove -y \
    && apt-get install -y --fix-missing \
        iptables \
        logrotate \
        # For some python libraries
        libssl-dev libcurl4-openssl-dev libpcre3-dev libpcre3 libyaml-dev libffi-dev \
        # For Oracle (+ `unzip` which is in the base already)
        libaio1 \
        # For MySQL
        libmysqlclient21=8.0.19-0ubuntu5 \
        libmysqlclient-dev=8.0.19-0ubuntu5 \
        default-libmysqlclient-dev \
        # For MSSQL
        freetds-dev tdsodbc unixodbc unixodbc-dev libtcmalloc-minimal4 \
        # For Postgres
        libpq-dev \
        # For Clickhouse
        clickhouse-client clickhouse-common-static \
        # logs
        yandex-push-client=6.70.1 \
        # nginx
        nginx \
        # portoctl
        yandex-porto \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && rm -rf /etc/nginx
# ^ nginx default configs are not useful here, so removing them completely;
# see also: `etc/nginx/`, `etc/service/nginx/`.


# ### bi database libraries ###

# Configure libraries for MSSQL
RUN cat /usr/share/tdsodbc/odbcinst.ini >> /etc/odbcinst.ini
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu/odbc

# See also: https://st.yandex-team.ru/BI-1585#5ed122525117d84ced2e3a40
# See also: ./oracle_libs.tgz.external
# ADD https://proxy.sandbox.yandex-team.ru/1539662156 /opt/oracle-tools  # but it doesn't get properly detected as an archive
RUN wget \
        -O /tmp/oracle_libs.tgz \
        --progress=dot:giga \
        https://proxy.sandbox.yandex-team.ru/1539662156 \
    && mkdir /opt/oracle-tools \
    && cd /opt/oracle-tools \
    && tar -xvf /tmp/oracle_libs.tgz \
    && rm /tmp/oracle_libs.tgz
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/opt/oracle-tools
ENV NLS_LANG .AL32UTF8


# ### py ###

# Disable (by default):
# version check: pip is pinned for reproducible builds, no need to point it out on each run.
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
# cache: not useful in docker. Enable explicitly where useful.
# In case this doesn't work, re-check https://github.com/pypa/pip/issues/5735
ENV PIP_NO_CACHE_DIR=1

# Mostly for tests and debug (which should be done in the same image as the rollout)
# but j2cli we use for building some configs (nginx/jaeger/syslog/etc)
RUN pip3 install -i https://pypi.yandex-team.ru/simple -U tox==3.20.1 j2cli==0.3.10 tox-pip-version==0.0.7 \
    && if [ -e /root/.cache ]; then rm -rf /root/.cache; fi


# ### etcetera ###

# For details, see https://a.yandex-team.ru/arc/trunk/arcadia/junk/hhell/datalens/syslog_ng_backport/build/build.sh?rev=7327877#L42
# Ubuntu bionic syslog-ng packaging doesn't build syslog-ng-mod-http.
# RUN wget --progress=dot:giga -O /usr/lib/syslog-ng/3.13/libhttp.so https://proxy.sandbox.yandex-team.ru/1714584219
# But actually, need a more fresh syslog-ng anyway (3.27 in this deb).
# Might not be needed after the switch to ubuntu focal (20.04) (it includes /usr/lib/syslog-ng/3.25/libhttp.so)
#RUN wget --progress=dot:giga -O /tmp/syslog-ng-core.deb https://proxy.sandbox.yandex-team.ru/1718237558 \
#    && wget --progress=dot:giga -O /tmp/syslog-ng-mod-http.deb https://proxy.sandbox.yandex-team.ru/1718237709 \
#    && dpkg -r syslog-ng-core syslog-ng-mod-journal \
#    && dpkg -i --force-confold /tmp/syslog-ng-core.deb /tmp/syslog-ng-mod-http.deb \
#    && rm  /tmp/syslog-ng-core.deb /tmp/syslog-ng-mod-http.deb

# Jaeger agent
# See https://st.yandex-team.ru/BI-1967#6011a462a57fa14c42dba9a7
RUN wget \
        --progress=dot:giga \
        -O /usr/local/bin/jaeger-agent \
        https://proxy.sandbox.yandex-team.ru/1956454766 \
    && chmod +x /usr/local/bin/jaeger-agent

ENV REQUESTS_CA_BUNDLE "/etc/ssl/certs/ca-certificates.crt"

RUN useradd -ms /bin/bash ext_query_user

COPY ./bin/ /usr/local/bin/
# In base image /etc/service is symlink to /etc/runit/runsvdir/default
#  New versions of Docker fails on such conflicts (however previous version just removes sym-link)
RUN rm -rf /etc/service
COPY ./etc/ /etc/
# For reference about `YandexCLCA.pem`:
# `RUN wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O /etc/YandexCLCA.pem`
# TODO?: put it into `/etc/ssl/certs/`?
COPY ./juggler/ /juggler/


# ### deb, logs ###

RUN mkdir -p /var/log/app

# Logrotate 'fix': add nocreate to logrotate configs because logrotate running
# under juggler-client is unable, for whatever reason, to chown the newly
# created files.
# Along the way, and for this reason as well, disable compression.
# Should be done after all the apt installs.
# Idempotent sed command.
RUN sed -r -i '/^[ \t]*nocreate/ d; /^[ \t]*create/ d; /^[ \t]*compress/ d; s/^([ \t]*)(rotate .*)$/\1\2\n\1nocreate/' /etc/logrotate.conf /etc/logrotate.d/*

# Logrotate fix 02:
# logrotate has a nasty habit of saying 'error: Ignoring app because of bad file
# mode - must be 0644 or 0444.', and not even returning a non-zero exit with it.
# And the build environments have a nasty habit of having `umask 0002` rather
# than `umask 0022`.
# So just ensure the very important files have the permissions logrotate expects.
RUN chmod 644 /etc/logrotate.d/*

# Since we are going to have different images for different installations someday
# let's put nebius-israel cert into the common pile as for now
COPY ./certs/NebiusILInternalRootCA.crt /usr/local/share/ca-certificates/
COPY ./certs/NemaxInternalRootCA.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates
