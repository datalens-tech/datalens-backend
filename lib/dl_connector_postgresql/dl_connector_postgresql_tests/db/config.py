import os
from typing import ClassVar

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_testing.containers import get_test_container_hostport

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D


# Infra settings
CORE_TEST_CONFIG = CoreTestEnvironmentConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=52311).host,
    port_us_http=get_test_container_hostport("us", fallback_port=52311).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=52310).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=52310).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["postgresql"],
    redis_host=get_test_container_hostport("redis-caches").host,
    redis_port=get_test_container_hostport("redis-caches", fallback_port=52312).port,
)


def get_postgres_ssl_ca() -> str:
    path = os.path.join(os.path.dirname(__file__), "../../docker-compose/db-postgres/ssl", "root.crt")

    with open(path) as f:
        return f.read()


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = "test_data"
    HOST: ClassVar[str] = get_test_container_hostport("db-postgres-13", fallback_port=52301).host
    PORT: ClassVar[int] = get_test_container_hostport("db-postgres-13", fallback_port=52301).port
    USERNAME: ClassVar[str] = "datalens"
    PASSWORD: ClassVar[str] = "qwerty"


class CoreSslConnectionSettings:
    DB_NAME: ClassVar[str] = "test_data"
    HOST: ClassVar[str] = "127.0.0.1"
    PORT: ClassVar[int] = 52303
    USERNAME: ClassVar[str] = "datalens"
    PASSWORD: ClassVar[str] = "qwerty"


SUBSELECT_QUERY_FULL = r"""
with base as (
    select generate_series(0, 6) as number
)
select
    number,
    'test' || number::text as str,
    number::bool as num_bool,
    number::text::bytea as num_bytea,
    number::char as num_char,
    -- number::name as num_name,
    number::int8 as num_int8,
    number::int2 as num_int2,
    number::int4 as num_int4,
    number::text as num_text,
    number::oid as num_oid,
    number::text::json as num_json,
    -- number::text::jsonb as num_jsonb, -- tests-pg too old
    number::float4 as num_float4,
    number::float8 as num_float8,
    number::numeric as num_numeric,
    (number || ' second')::interval as num_interval,
    number::varchar(12) as num_varchar,
    ('2020-01-0' || (number + 1))::date as num_date,
    ('00:01:0' || number)::time as num_time,
    ('2020-01-01T00:00:0' || number)::timestamp as num_timestamp,
    ('2020-01-01T00:00:0' || number)::timestamptz as num_timestamptz,
    ARRAY[number, 2, 3] as num_array,
    'nan'::double precision + number as some_nan
from base
limit 10
"""
DASHSQL_QUERY = """select
    '1' as aa,
    ARRAY[2, 3] as bb,
    unnest(ARRAY[4, 5]) as cc,
    '2020-01-01 01:02:03'::timestamp as dd,
    '2020-01-01 02:03:0'::timestamptz as ee,
    'zxc'::bytea as ff
"""
QUERY_WITH_PARAMS = r"""
select
  'normal '':string'''::text as v1_normal_string,
  E'extended:string\nwith\nnewlines'::text as v2_ext_string,
  {{some_string}}::text as v3_param_string,
  {{some_integer}} as v4_param_integer,
  {{some_float}} as v5_param_float,
  {{some_boolean}} as v6_param_boolean,
  {{some_other_boolean}} as v7_param_boolean,
  {{some_date}} as v8_param_date,
  {{some_datetime}} as v9_param_datetime,
  {{3xtr4 ше1гd param}}::text as v10_weird_name,
  {{3xtr4 же1гd param}}::text as v11_weird_name,
  1 in {{intvalues}} as v12_int_in,
  0 in {{intvalues}} as v13_int_in_2,
  'a' in {{strvalues}} as v14_str_in,
  'z' in {{strvalues}} as v14_str_in_2,
  1 as stuff
"""

DB_URLS = {
    D.POSTGRESQL_9_3: (
        f'bi_postgresql://datalens:qwerty@{get_test_container_hostport("db-postgres-9-3", fallback_port=52300).as_pair()}/test_data'
    ),
    D.POSTGRESQL_9_4: (
        f'bi_postgresql://datalens:qwerty@{get_test_container_hostport("db-postgres-13", fallback_port=52301).as_pair()}/test_data'
    ),
    D.COMPENG: (
        f'bi_postgresql://datalens:qwerty@{get_test_container_hostport("db-postgres-13", fallback_port=52301).as_pair()}/test_data'
    ),
}
DB_CORE_URL = DB_URLS[D.POSTGRESQL_9_4]
DB_CORE_SSL_URL = "bi_postgresql://datalens:qwerty@localhost:52303/test_data"

API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["postgresql"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
