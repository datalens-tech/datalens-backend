from typing import ClassVar

from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=50300).host,
    port_us_http=get_test_container_hostport("us", fallback_port=50300).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=50309).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=50309).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["clickhouse"],
)


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = "test_data"
    HOST: ClassVar[str] = get_test_container_hostport("db-clickhouse", fallback_port=50310).host
    PORT: ClassVar[int] = get_test_container_hostport("db-clickhouse", fallback_port=50310).port
    USERNAME: ClassVar[str] = "datalens"
    PASSWORD: ClassVar[str] = "qwerty"


DB_CORE_URL = (
    f"clickhouse://datalens:qwerty@"
    f'{get_test_container_hostport("db-clickhouse", fallback_port=50310).as_pair()}/test_data'
)
