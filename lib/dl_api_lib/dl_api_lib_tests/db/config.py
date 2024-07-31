from typing import ClassVar

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_configs.connector_availability import (
    ConnectorAvailabilityConfigSettings,
    ConnectorSettings,
    SectionSettings,
    TranslatableSettings,
)
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_testing.containers import get_test_container_hostport

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D


CORE_TEST_CONFIG = CoreTestEnvironmentConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=52500).host,
    port_us_http=get_test_container_hostport("us", fallback_port=52500).port,
    host_us_pg=get_test_container_hostport("db-postgres", fallback_port=52509).host,
    port_us_pg_5432=get_test_container_hostport("db-postgres", fallback_port=52509).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["clickhouse", "postgresql"],
    compeng_url=(
        f"postgresql://us:us@"
        f'{get_test_container_hostport("db-postgres", fallback_port=52509).as_pair()}/us-db-ci_purgeable'
    ),
    redis_host=get_test_container_hostport("redis-caches").host,
    redis_port=get_test_container_hostport("redis-caches", fallback_port=52505).port,
)


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = "test_data"
    HOST: ClassVar[str] = get_test_container_hostport("db-clickhouse", fallback_port=52510).host
    PORT: ClassVar[int] = get_test_container_hostport("db-clickhouse", fallback_port=52510).port
    USERNAME: ClassVar[str] = "datalens"
    PASSWORD: ClassVar[str] = "qwerty"


DB_URLS = {
    D.CLICKHOUSE_22_10: f"clickhouse://datalens:qwerty@"
    f'{get_test_container_hostport("db-clickhouse", fallback_port=52510).as_pair()}/test_data',
}
DB_CORE_URL = DB_URLS[D.CLICKHOUSE_22_10]

API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["clickhouse", "postgresql"],
    core_test_config=CORE_TEST_CONFIG,
    connector_availability_settings=ConnectorAvailabilityConfigSettings(
        uncategorized=[ConnectorSettings(conn_type="postgres")],
        sections=[
            SectionSettings(
                title_translatable=TranslatableSettings(text="ClickHouse®"),
                connectors=[ConnectorSettings(conn_type="clickhouse")],
            )
        ],
    ),
    ext_query_executer_secret_key="_some_test_secret_key_",
)
