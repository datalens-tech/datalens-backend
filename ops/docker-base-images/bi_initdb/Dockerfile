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


ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu/odbc

ENV PATH $PATH:/opt/oracle-tools
ENV SQLPATH $SQLPATH:/opt/oracle-tools
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/opt/oracle-tools

# Save script for waiting sockets
COPY initdb /opt/initdb

CMD /opt/initdb/entrypoint.sh
