import os
from typing import ClassVar

from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport

from bi_connector_mssql.formula.constants import MssqlDialect as D


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=52111).host,
    port_us_http=get_test_container_hostport("us", fallback_port=52111).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=52110).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=52110).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["mssql"],
)

COMPOSE_PROJECT_NAME = os.environ.get("COMPOSE_PROJECT_NAME", "bi_connector_mssql")
MSSQL_CONTAINER_LABEL = "db-mssql-14"
INIT_DB_PORT = get_test_container_hostport("init-db", fallback_port=52101).port


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = "test_data"
    HOST: ClassVar[str] = get_test_container_hostport("db-mssql-14", fallback_port=52100).host
    PORT: ClassVar[int] = get_test_container_hostport("db-mssql-14", fallback_port=52100).port
    USERNAME: ClassVar[str] = "datalens"
    PASSWORD: ClassVar[str] = "qweRTY123"


SUBSELECT_QUERY_FULL = r"""
select
    number,
    cast(number as tinyint) as num_tinyint,
    cast(number as smallint) as num_smallint,
    cast(number as integer) as num_integer,
    cast(number as bigint) as num_bigint,
    cast(number as float) as num_float,
    cast(number as real) as num_real,
    cast(number as numeric) as num_numeric,
    cast(number as decimal) as num_decimal,
    cast(number as bit) as num_bit,
    cast(number as char) as num_char,
    cast(number as varchar) as num_varchar,
    cast(cast(number as varchar) as text) as num_text,
    cast(number as nchar) as num_nchar,
    cast(number as nvarchar) as num_nvarchar,
    cast(cast(number as nvarchar) as ntext) as num_ntext,
    cast(concat('2020-01-0', number + 1) as date) as num_date,
    cast(number as datetime) as num_datetime,
    cast(concat('2020-01-01T00:00:0',
    number) as datetime2) as num_datetime2,
    cast(number as smalldatetime) as num_smalldatetime,
    cast(concat('2020-01-01T00:00:00+00:0',
    number) as datetimeoffset) as num_datetimeoffset,
    NEWID() as uuid
from
    (
        select 0 as number
        union all
        select 1 as number
        union all
        select 6 as number
    ) as base
"""

_DB_URL = (
    f'mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BFreeTDS%7D%3BServer%3D{get_test_container_hostport("db-mssql-14", fallback_port=52100).host}%3B'
    f'Port%3D{get_test_container_hostport("db-mssql-14", fallback_port=52100).port}%3BDatabase%3Dtest_data%3BUID%3Ddatalens%3BPWD%3DqweRTY123%3BTDS_Version%3D8.0'
)
DB_CORE_URL = _DB_URL
DB_URLS = {
    D.MSSQLSRV_14_0: _DB_URL,
}

BI_TEST_CONFIG = BiApiTestEnvironmentConfiguration(
    api_connector_ep_names=["mssql"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
