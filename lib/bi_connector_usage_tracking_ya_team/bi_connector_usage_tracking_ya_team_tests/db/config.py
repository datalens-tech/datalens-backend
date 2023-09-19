from typing import ClassVar

from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=51911).host,
    port_us_http=get_test_container_hostport("us", fallback_port=51911).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=51910).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=51910).port,
    us_master_token="AC1ofiek8coB",
)


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = "test_data"
    ENDPOINT: ClassVar[str] = "localhost"
    CLUSTER_NAME: ClassVar[str] = "default"
    MAX_EXECUTION_TIME: ClassVar[int] = 60


DB_CORE_URL = f'clickhouse://datalens:qwerty@{get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).as_pair()}/test_data'
