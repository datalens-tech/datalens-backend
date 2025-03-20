import os
from typing import ClassVar

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_testing.containers import get_test_container_hostport

from dl_connector_oracle.formula.constants import OracleDialect as D


# Infra settings
CORE_TEST_CONFIG = CoreTestEnvironmentConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=51811).host,
    port_us_http=get_test_container_hostport("us", fallback_port=51811).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=51810).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=51810).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["oracle"],
    redis_host=get_test_container_hostport("redis-caches").host,
    redis_port=get_test_container_hostport("redis-caches", fallback_port=51812).port,
)

COMPOSE_PROJECT_NAME = os.environ.get("COMPOSE_PROJECT_NAME", "dl_connector_oracle")
ORACLE_CONTAINER_LABEL = "db-oracle"


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = "XEPDB1"
    HOST: ClassVar[str] = get_test_container_hostport("db-oracle", fallback_port=51800).host
    PORT: ClassVar[int] = get_test_container_hostport("db-oracle", fallback_port=51800).port
    USERNAME: ClassVar[str] = "datalens"
    PASSWORD: ClassVar[str] = "qwerty"
    SSL_ENABLE: ClassVar[bool] = False


class CoreSSLConnectionSettings:
    DB_NAME: ClassVar[str] = "XEPDB1"
    HOST: ClassVar[str] = get_test_container_hostport("db-oracle-ssl", fallback_port=51801).host
    PORT: ClassVar[int] = get_test_container_hostport("db-oracle-ssl", fallback_port=51801).port
    USERNAME: ClassVar[str] = "datalens"
    PASSWORD: ClassVar[str] = "qwerty"
    SSL_ENABLE: ClassVar[bool] = True
    CERT_PROVIDER_URL: ClassVar[
        str
    ] = f"http://{get_test_container_hostport('ssl-provider', fallback_port=8080).as_pair()}"


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
QUERY_WITH_PARAMS = r"""
select
  'normal '':string''' as v1_normal_string,
  'extended:string' || chr(10) || 'with' || chr(10) || 'newlines' as v2_ext_string,
  {{some_string}} as v3_param_string,
  {{some_integer}} as v4_param_integer,
  {{some_float}} as v5_param_float,
  {{some_boolean}} as v6_param_boolean,
  {{some_other_boolean}} as v7_param_boolean,
  {{some_date}} as v8_param_date,
  {{some_datetime}} as v9_param_datetime,
  {{3xtr4 ше1гd param}} as v10_weird_name,
  {{3xtr4 же1гd param}} as v11_weird_name,
  case when 1 in (1, 2) then 1 else 0 end as v12_int_in_tst,
  case when 1 in {{intvalues}} then 1 else 0 end as v12_int_in,
  case when 0 in {{intvalues}} then 1 else 0 end as v13_int_in_2,
  case when 'a' in ('z', 'x') then 1 else 0 end as v14_str_in_tst,
  case when 'a' in {{strvalues}} then 1 else 0 end as v14_str_in,
  case when 'z' in {{strvalues}} then 1 else 0 end as v14_str_in_2,
  1 as stuff
FROM dual
"""

_DB_URL = (
    f"oracle://datalens:qwerty@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={CoreConnectionSettings.HOST})"
    f"(PORT={CoreConnectionSettings.PORT}))(CONNECT_DATA=(SERVICE_NAME={CoreConnectionSettings.DB_NAME})))"
)
SYSDBA_URL = (
    f"oracle://sys:qwerty@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={CoreConnectionSettings.HOST})"
    f"(PORT={CoreConnectionSettings.PORT}))(CONNECT_DATA=(SERVICE_NAME={CoreConnectionSettings.DB_NAME})))?mode=sysdba"
)
DB_CORE_URL = _DB_URL
DB_URLS = {
    D.ORACLE_12_0: _DB_URL,
}

_DB_URL_SSL = (
    f"oracle://datalens:qwerty@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST={CoreSSLConnectionSettings.HOST})"
    f"(PORT={CoreSSLConnectionSettings.PORT}))(CONNECT_DATA=(SERVICE_NAME={CoreSSLConnectionSettings.DB_NAME})))"
)
SYSDBA_URL_SSL = (
    f"oracle://sys:qwerty@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST={CoreSSLConnectionSettings.HOST})"
    f"(PORT={CoreSSLConnectionSettings.PORT}))(CONNECT_DATA=(SERVICE_NAME={CoreSSLConnectionSettings.DB_NAME})))?mode=sysdba"
)
DB_CORE_URL_SSL = _DB_URL_SSL
DB_URLS_SSL = {
    D.ORACLE_12_0: _DB_URL_SSL,
}

API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["oracle"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
