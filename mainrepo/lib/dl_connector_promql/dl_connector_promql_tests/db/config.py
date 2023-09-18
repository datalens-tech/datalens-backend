from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
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


BI_TEST_CONFIG = BiApiTestEnvironmentConfiguration(
    bi_api_connector_whitelist=["promql"],
    core_connector_whitelist=["promql"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)

PROMETHEUS_CONTAINER_HOSTPORT = get_test_container_hostport("db-prometheus", fallback_port=50518)
API_CONNECTION_SETTINGS = dict(
    host=PROMETHEUS_CONTAINER_HOSTPORT.host,
    port=PROMETHEUS_CONTAINER_HOSTPORT.port,
    username="admin",
    password="admin",
)
