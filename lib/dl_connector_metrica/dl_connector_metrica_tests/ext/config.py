from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_testing.containers import get_test_container_hostport


METRIKA_SAMPLE_COUNTER_ID = "44147844"
APPMETRICA_SAMPLE_COUNTER_ID = "1111"

# Infra settings
CORE_TEST_CONFIG = CoreTestEnvironmentConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=51911).host,
    port_us_http=get_test_container_hostport("us", fallback_port=51911).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=51910).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=51910).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["metrica_api", "appmetrica_api", "testing"],
)

API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["metrica_api", "appmetrica_api"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
