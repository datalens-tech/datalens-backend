from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=51600).host,
    port_us_http=get_test_container_hostport("us", fallback_port=51600).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=51609).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=51609).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["bigquery"],
    redis_host=get_test_container_hostport("redis-caches").host,
    redis_port=get_test_container_hostport("redis-caches", fallback_port=51612).port,
)


API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["bigquery"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
