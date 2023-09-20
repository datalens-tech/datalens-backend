import os
from typing import ClassVar

from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport

from bi_connector_oracle.formula.constants import OracleDialect as D


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=51811).host,
    port_us_http=get_test_container_hostport("us", fallback_port=51811).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=51810).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=51810).port,
    us_master_token="AC1ofiek8coB",
)

COMPOSE_PROJECT_NAME = os.environ.get("COMPOSE_PROJECT_NAME", "bi_connector_oracle")
ORACLE_CONTAINER_LABEL = "db-oracle"
INIT_DB_PORT = get_test_container_hostport("init-db", fallback_port=51802).port


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = "XEPDB1"
    HOST: ClassVar[str] = get_test_container_hostport("db-oracle", fallback_port=51800).host
    PORT: ClassVar[int] = get_test_container_hostport("db-oracle", fallback_port=51800).port
    USERNAME: ClassVar[str] = "datalens"
    PASSWORD: ClassVar[str] = "qwerty"


DEFAULT_ORACLE_SCHEMA_NAME = "DATALENS"


SUBSELECT_QUERY_FULL = r"""
select
    num,
    'test' || num as num_str,
    cast(num as integer) as num_integer,
    cast(num as number) as num_number,
    -- cast(num as number(9,9)) as num_number_9_9,
    cast(num as binary_float) as num_binary_float,
    cast(num as binary_double) as num_binary_double,
    cast(num as char) as num_char,
    cast(num as varchar(3)) as num_varchar,
    cast(num as varchar2(4)) as num_varchar2,
    cast(num as nchar) as num_nchar,
    -- cast(num as nvarchar(5)) as num_nvarchar,
    cast(num as nvarchar2(5)) as num_nvarchar2,
    DATE '2020-01-01' + num as num_date,
    TIMESTAMP '1999-12-31 23:59:59.10' + numToDSInterval(num, 'second') as num_timestamp,
    TIMESTAMP '1999-12-31 23:59:59.10-07:00' + numToDSInterval(num, 'second') as num_timestamp_tz
from (
    select 0 as num from dual
    union all
    select 1 as num from dual
    union all
    select 6 as num from dual
) sq
"""

_DB_URL = (
    f'oracle://datalens:qwerty@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={get_test_container_hostport("db-oracle", fallback_port=51800).host})'
    f'(PORT={get_test_container_hostport("db-oracle", fallback_port=51800).port}))(CONNECT_DATA=(SERVICE_NAME={CoreConnectionSettings.DB_NAME})))'
)
DB_CORE_URL = _DB_URL
DB_URLS = {
    D.ORACLE_12_0: _DB_URL,
}

BI_TEST_CONFIG = BiApiTestEnvironmentConfiguration(
    api_connector_ep_names=["oracle"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
