import enum

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_testing.containers import get_test_container_hostport


class GreenplumVersion(enum.Enum):
    GP6 = "gp6"
    GP7 = "gp7"


# Infra settings
CORE_TEST_CONFIG = CoreTestEnvironmentConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=51911).host,
    port_us_http=get_test_container_hostport("us", fallback_port=51911).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=51910).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=51910).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["greenplum", "postgresql"],
    redis_host=get_test_container_hostport("redis-caches").host,
    redis_port=get_test_container_hostport("redis-caches", fallback_port=51912).port,
)

DB_NAME = "test_data"
TABLE_NAME = "sample"
GP_USER = "datalens"
GP_PASSWORD = "qwerty"


DB_URLS = {
    GreenplumVersion.GP6: f'bi_postgresql://{GP_USER}:{GP_PASSWORD}@{get_test_container_hostport("db-greenplum-6", fallback_port=52320).as_pair()}/{DB_NAME}',
    GreenplumVersion.GP7: f'bi_postgresql://{GP_USER}:{GP_PASSWORD}@{get_test_container_hostport("db-greenplum-7", fallback_port=52321).as_pair()}/{DB_NAME}',
}


CONNECTION_PARAMS_BY_VERSION = {
    GreenplumVersion.GP6: dict(
        db_name=DB_NAME,
        host=get_test_container_hostport("db-greenplum-6", fallback_port=52320).host,
        port=get_test_container_hostport("db-greenplum-6", fallback_port=52320).port,
        username=GP_USER,
        password=GP_PASSWORD,
    ),
    GreenplumVersion.GP7: dict(
        db_name=DB_NAME,
        host=get_test_container_hostport("db-greenplum-7", fallback_port=52321).host,
        port=get_test_container_hostport("db-greenplum-7", fallback_port=52321).port,
        username=GP_USER,
        password=GP_PASSWORD,
    ),
}

API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["greenplum", "postgresql"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)


DASHSQL_QUERY = r"""
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
