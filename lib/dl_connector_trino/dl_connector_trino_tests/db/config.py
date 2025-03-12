from typing import ClassVar

from trino.sqlalchemy import URL

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_testing.containers import get_test_container_hostport

from dl_connector_trino.core.constants import TrinoAuthType
from dl_connector_trino.formula.constants import TrinoDialect as D


CORE_TEST_CONFIG = CoreTestEnvironmentConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=21133).host,
    port_us_http=get_test_container_hostport("us", fallback_port=21133).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=22087).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=22087).port,
    redis_host=get_test_container_hostport("redis-caches", fallback_port=22569).host,
    redis_port=get_test_container_hostport("redis-caches", fallback_port=22569).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["trino"],
)


class BaseConnectionSettings:
    CATALOG_MEMORY: ClassVar[str] = "test_memory_catalog"
    CATALOG_MYSQL: ClassVar[str] = "test_mysql_catalog"
    SCHEMA: ClassVar[str] = "default"


class CoreConnectionSettings(BaseConnectionSettings):
    HOST: ClassVar[str] = get_test_container_hostport("trino-no-auth", fallback_port=21123).host
    PORT: ClassVar[int] = get_test_container_hostport("trino-no-auth", fallback_port=21123).port
    USERNAME: ClassVar[str] = "datalens"
    AUTH_TYPE: ClassVar[str] = TrinoAuthType.NONE


class CoreSslConnectionSettings(BaseConnectionSettings):
    HOST: ClassVar[str] = get_test_container_hostport("trino-tls-nginx", fallback_port=21124).host
    PORT: ClassVar[int] = get_test_container_hostport("trino-tls-nginx", fallback_port=21124).port
    USERNAME: ClassVar[str] = "trino_user"
    CERT_PROVIDER_URL: ClassVar[
        str
    ] = f"http://{get_test_container_hostport('ssl-provider', fallback_port=26002).as_pair()}"


class CorePasswordConnectionSettings(CoreSslConnectionSettings):
    PASSWORD: ClassVar[str] = "trino_password"
    AUTH_TYPE: ClassVar[str] = TrinoAuthType.PASSWORD


class CoreJwtConnectionSettings(CoreSslConnectionSettings):
    JWT: ClassVar[str] = (
        "ewogICJhbGciOiAiUlMyNTYiLAogICJ0eXAiOiAiSldUIgp9Cg.ewogICJpc3MiOiAidGVzdF9qd3RfaXNzdWVyIiwKICAiYXVkI"
        "jogInRlc3RfdHJpbm9fY29vcmRpbmF0b3IiLAogICJzdWIiOiAidHJpbm9fdXNlciIsCiAgImlhdCI6IDE3MzgzMzkxOTIKfQo.k"
        "hyhr2nRZuGRG2cKCEM5vlcOxV7dh4vMSzRQ1VLa6F5-KwvytVbhgO0VvEr0FDJTs8sekU56BtzhN-bfDijQIcVdP5ePojVjrwoHs"
        "haZIPmS0fhBpR8G9XAphIToBB-WtxW3bzAqVAsmUFctUSzRw7iKilcfzS6He8YhIqGOEQ7GFQxHHiquuwo0HoH60LuDoZVPhnIRw"
        "oKkShhF7QYokB2a-9nE8OUrhvfdy4Ix_9u_FnaToXZUhTomEIOV4Fut9QihslkSlKetTsZFtD5J5oZ292wa_mI8P_jE13tMP7iZl"
        "mUhNcNenmf7WsxqSP7SZDmItS-pPdfrAWSYw4EyHw"
    )
    AUTH_TYPE: ClassVar[str] = TrinoAuthType.JWT


SUBSELECT_QUERY_FULL = r"""
select
    number,
    cast(number as boolean) as num_bool,
    cast(number as tinyint) as num_tinyint,
    cast(number as smallint) as num_smallint,
    cast(number as integer) as num_integer,
    cast(number as bigint) as num_bigint,
    cast(number as real) as num_real,
    cast(number as double) as num_double,
    cast(number as decimal(9, 3)) as num_decimal_9_3,
    cast(char_value as char(3)) as char_3,
    cast(char_value as varchar(3)) as varchar_3,
    cast(json_string as json) as json,
    cast(date_value as date) as date,
    cast(time_value as time) as time,
    cast(times_with_timezone as time with time zone) as time_with_timezone,
    cast(timestamp_value as timestamp) as timestamp,
    cast(timestamp_with_timezone as timestamp with time zone) as timestamp_with_timezone
from
    (
        select 
            0 as number, 
            '0' as char_value,
            '{["0"]}' as json_string,
            '2025-01-01' as date_value,
            '00:00:00' as time_value,
            '01:02:03.456 -08:00' as times_with_timezone,
            '2025-01-01 00:00:00' as timestamp_value,
            '2001-08-22 03:04:05.321 America/New_York' as timestamp_with_timezone
        union all
        select 
            1, 
            '1',
            '{["1"]}',
            '2025-01-02',
            '00:00:01',
            '01:02:03.456 -08:00',
            '2025-01-02 00:00:01',
            '2025-01-02 00:00:01.000 America/Denver'
        union all
        select 
            6, 
            '6',
            '{["6"]}',
            '2025-01-07',
            '00:00:06',
            '01:02:03.456 -08:00',
            '2025-01-07 00:00:06',
            '2025-01-07 00:00:06.000 America/Los_Angeles'
    ) as base
"""

DB_URLS = {
    (D.TRINO, "memory_catalog"): URL(
        host=CoreConnectionSettings.HOST,
        port=CoreConnectionSettings.PORT,
        user="tests_init_worker",
        catalog=CoreConnectionSettings.CATALOG_MEMORY,
    ),
    (D.TRINO, "mysql_catalog"): URL(
        host=CoreConnectionSettings.HOST,
        port=CoreConnectionSettings.PORT,
        user="tests_init_worker",
        catalog=CoreConnectionSettings.CATALOG_MYSQL,
    ),
    (D.TRINO, "ssl"): URL(
        host=CoreSslConnectionSettings.HOST,
        port=CoreSslConnectionSettings.PORT,
        user=CoreSslConnectionSettings.USERNAME,
        catalog=CoreSslConnectionSettings.CATALOG_MYSQL,
    ),
}
DB_CORE_URL_MEMORY_CATALOG = DB_URLS[(D.TRINO, "memory_catalog")]
DB_CORE_URL_MYSQL_CATALOG = DB_URLS[(D.TRINO, "mysql_catalog")]
DB_CORE_SSL_URL = DB_URLS[(D.TRINO, "ssl")]

API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["trino"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
