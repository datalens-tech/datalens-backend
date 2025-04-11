#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

RUN apt-get update \
    && apt-get -y upgrade -o Dpkg::Options::="--force-confold" \
    && apt-get install -y --fix-missing \
        # For Oracle
        libaio1 \
        # For MSSQL
        freetds-dev tdsodbc unixodbc unixodbc-dev libtcmalloc-minimal4 \
        # For Postgres
        libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Configure libraries for MSSQL
RUN cat /usr/share/tdsodbc/odbcinst.ini >> /etc/odbcinst.ini
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu/odbc
