#syntax=docker/dockerfile:1.4
FROM bake_ctx_base_img

RUN echo 'deb https://repo.yandex.ru/clickhouse/deb stable main' >> /etc/apt/sources.list.d/clickhouse.list \
    && echo 'deb https://mirror.yandex.ru/mirrors/postgresql focal-pgdg main' > /etc/apt/sources.list.d/pgdg.list \
    && echo 'deb http://dist.yandex.ru/search-focal/ stable/amd64/' >> /etc/apt/sources.list.d/yandex.list \
    && echo 'deb http://dist.yandex.ru/search-focal/ stable/all/' >> /etc/apt/sources.list.d/yandex.list \
    && apt-get update \
    && apt-get -y upgrade -o Dpkg::Options::="--force-confold" \
    && apt-get install -y --fix-missing \
        # For Oracle
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
    && apt-get clean && rm -rf /var/lib/apt/lists/*
