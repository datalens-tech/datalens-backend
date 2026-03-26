from typing import ClassVar

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_testing.containers import get_test_container_hostport

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


# Infra settings
CORE_TEST_CONFIG = CoreTestEnvironmentConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=59311).host,
    port_us_http=get_test_container_hostport("us", fallback_port=59311).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=59310).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=59310).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["starrocks"],
    redis_host=get_test_container_hostport("redis-caches").host,
    redis_port=get_test_container_hostport("redis-caches", fallback_port=59312).port,
)


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = "test_data"
    HOST: ClassVar[str] = get_test_container_hostport("db-starrocks-3", fallback_port=59030).host
    PORT: ClassVar[int] = get_test_container_hostport("db-starrocks-3", fallback_port=59030).port
    USERNAME: ClassVar[str] = "root"
    PASSWORD: ClassVar[str] = ""


DB_URLS = {
    D.STARROCKS_3_0: f"bi_starrocks://root@{get_test_container_hostport('db-starrocks-3', fallback_port=59030).as_pair()}/test_data"
}
DB_CORE_URL = DB_URLS[D.STARROCKS_3_0]

API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["starrocks"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
