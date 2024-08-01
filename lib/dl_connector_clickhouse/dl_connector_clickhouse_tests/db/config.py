import os
from typing import ClassVar

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_testing.containers import get_test_container_hostport

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D


# Infra settings
SSL_CA_FILENAME = "marsnet_ca.crt"


def get_clickhouse_ssl_ca_path() -> str:
    return os.path.join(os.path.dirname(__file__), "../../docker-compose/db-clickhouse/ssl", SSL_CA_FILENAME)


def get_clickhouse_ssl_ca() -> str:
    path = get_clickhouse_ssl_ca_path()

    with open(path) as f:
        return f.read()


CORE_TEST_CONFIG = CoreTestEnvironmentConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=52211).host,
    port_us_http=get_test_container_hostport("us", fallback_port=52211).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=52210).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=52210).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["clickhouse"],
    redis_host=get_test_container_hostport("redis-caches").host,
    redis_port=get_test_container_hostport("redis-caches", fallback_port=52212).port,
)


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = "test_data"
    HOST: ClassVar[str] = get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).host
    PORT: ClassVar[int] = get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).port
    USERNAME: ClassVar[str] = "datalens"
    PASSWORD: ClassVar[str] = "qwerty"


class CoreSslConnectionSettings:
    DB_NAME: ClassVar[str] = "test_data"
    # don't use get_test_container_hostport, localhost in needed due to IP usage deprecation in CN
    HOST: ClassVar[str] = "localhost"
    PORT: ClassVar[int] = 52206
    USERNAME: ClassVar[str] = "datalens"
    PASSWORD: ClassVar[str] = "qwerty"


DASHSQL_QUERY = r"""
select
    arrayJoin([11, 22, NULL]) as a,
    [33, 44] as b,
    toDateTime('2020-01-02 03:04:05', 'UTC') + a as ts
"""
DASHSQL_QUERY_FULL = r"""
select
    arrayJoin(range(7)) as number,
    'test' || toString(number) as str,
    cast(number as Int64) as num_int64,
    cast(number as Int32) as num_int32,
    cast(number as Int16) as num_int16,
    cast(number as Int8) as num_int8,
    cast(number as UInt64) as num_uint64,
    cast(number as UInt32) as num_uint32,
    cast(number as UInt16) as num_uint16,
    cast(number as Nullable(UInt8)) as num_uint8_n,
    cast(number as Nullable(Date)) as num_date,
    cast(number as Nullable(DateTime)) as num_datetime,
    cast(number as Float64) as num_float64,
    cast(number as Nullable(Float32)) as num_float32_n,
    cast(number as Decimal(3, 3)) as num_decimal,
    cast(number as String) as num_string,
    cast('bcc3de04-d31a-4e17-8485-8ef423f646be' as UUID) as num_uuid,
    cast(number as IPv4) as num_ipv4,
    cast('20:43:ff::40:1bc' as IPv6) as num_ipv6,
    cast(toString(number) as FixedString(10)) as num_fixedstring,
    cast(number as Enum8('a'=0, 'b'=1, 'c'=2, 'd'=3, 'e'=4, 'f'=5, 'g'=6)) as num_enum8,
    cast(number as Enum16('a'=0, 'b'=1, 'c'=2, 'd'=3, 'e'=4, 'f'=5, 'g'=6)) as num_enum16,
    (number, 'x') as num_tuple,
    [number, -2] as num_intarray,
    [toString(number), '-2'] as num_strarray,
    cast(toString(number) as LowCardinality(Nullable(String))) as num_lc,
    cast(number as DateTime('Pacific/Chatham')) as num_dt_tz,
    cast(number as DateTime64(6)) as num_dt64,
    cast(number as DateTime64(6, 'America/New_York')) as num_dt64_tz
limit 10
"""


DB_URLS = {
    D.CLICKHOUSE_21_8: f"clickhouse://datalens:qwerty@"
    f'{get_test_container_hostport("db-clickhouse-21-8", fallback_port=52202).as_pair()}/test_data',
    D.CLICKHOUSE_22_10: f"clickhouse://datalens:qwerty@"
    f'{get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).as_pair()}/test_data',
}
DB_CORE_URL = DB_URLS[D.CLICKHOUSE_22_10]

API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["clickhouse"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)

# don't use get_test_container_hostport, localhost in needed due to IP usage deprecation in CN
DB_CORE_SSL_URL = "clickhouse://datalens:qwerty@localhost:52206/test_data?protocol=https"
