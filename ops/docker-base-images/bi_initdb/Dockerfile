FROM registry.yandex.net/ubuntu:xenial

# #######
# Minimal(-ish) image that is capable of connecting to mssql and oracle for
# checking their aliveness (while waiting while they boot up).
# #######

env DEBIAN_FRONTEND noninteractive
env container docker
run locale-gen en_US.UTF-8 && update-locale LANG=en_US.UTF-8

RUN apt-get update -y \
    && apt-get install -y apt-utils \
    && apt-get install -y libaio1 unzip python3 \
    && apt-get clean

# # Install MSSQL tools (libraries and CLI)
# # Right-from-the-source external-internet-access version,
# # to be used for updates / as reference / when the debs cache is not enough:
# RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
#     # HTTPS doesn't really work for the MS repo, so replace with HTTP
#     && curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list \
#      | sed s/https/http/ \
#      | tee /etc/apt/sources.list.d/msprod.list \
#     && apt-get update && ACCEPT_EULA=Y apt-get install -y \
#         mssql-tools freetds-dev tdsodbc unixodbc unixodbc-dev \
#     && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
#     && locale-gen en_US.UTF-8 && update-locale \
#     && cat /usr/share/tdsodbc/odbcinst.ini >> /etc/odbcinst.ini

# # git submodule debs version:
# COPY ./external-libs/mssql /tmp/mssqldeb
# RUN apt-get update -y && \
#     apt-get install -y libltdl3-dev tdsodbc \
#     && ACCEPT_EULA=Y dpkg -i /tmp/mssqldeb/*.deb \
#     && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
#     && cat /usr/share/tdsodbc/odbcinst.ini >> /etc/odbcinst.ini \
#     && rm -rf /tmp/mssqldeb \
#     && apt-get clean

# # sandbox resource version:
# See also: https://st.yandex-team.ru/BI-1585#5ed122525117d84ced2e3a40
# See also: ./mssql_libs.tgz.external
RUN wget \
        -O /tmp/mssql_deb.tgz \
        --progress=dot:giga \
        https://proxy.sandbox.yandex-team.ru/1539658731 \
    && mkdir /tmp/mssql_deb \
    && cd /tmp/mssql_deb \
    && tar -xvf /tmp/mssql_deb.tgz \
    && rm /tmp/mssql_deb.tgz \
    && apt-get update -y \
    && apt-get install -y libltdl3-dev tdsodbc \
    && ACCEPT_EULA=Y dpkg -i /tmp/mssql_deb/*.deb \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
    && cat /usr/share/tdsodbc/odbcinst.ini >> /etc/odbcinst.ini \
    && rm -rf /tmp/mssql_deb \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu/odbc

# Install Oracle tools (libraries and CLI)
# See also: https://st.yandex-team.ru/BI-1585#5ed122525117d84ced2e3a40
# See also: ./oracle_libs.tgz.external
# See also: ./oracle_cli.tgz.external
RUN wget \
        -O /tmp/oracle_libs.tgz \
        --progress=dot:giga \
        https://proxy.sandbox.yandex-team.ru/1539662156 \
    && wget \
        -O /tmp/oracle_cli.tgz \
        --progress=dot:giga \
        https://proxy.sandbox.yandex-team.ru/1539663420 \
    && mkdir /opt/oracle-tools \
    && cd /opt/oracle-tools \
    && tar -xvf /tmp/oracle_libs.tgz \
    && tar -xvf /tmp/oracle_cli.tgz \
    && rm /tmp/oracle_libs.tgz \
    && rm /tmp/oracle_cli.tgz

ENV PATH $PATH:/opt/oracle-tools
ENV SQLPATH $SQLPATH:/opt/oracle-tools
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/opt/oracle-tools

# Save script for waiting sockets
COPY initdb /opt/initdb

CMD /opt/initdb/entrypoint.sh
