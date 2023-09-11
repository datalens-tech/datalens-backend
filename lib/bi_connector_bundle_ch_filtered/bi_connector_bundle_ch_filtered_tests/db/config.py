from typing import ClassVar

from bi_core_testing.configuration import DefaultCoreTestConfiguration
from bi_api_lib_testing_ya.configuration import BiApiTestEnvironmentConfigurationPrivate
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistryFactory
from bi_testing.containers import get_test_container_hostport


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport('us', fallback_port=51911).host,
    port_us_http=get_test_container_hostport('us', fallback_port=51911).port,
    host_us_pg=get_test_container_hostport('pg-us', fallback_port=51910).host,
    port_us_pg_5432=get_test_container_hostport('pg-us', fallback_port=51910).port,
    us_master_token='AC1ofiek8coB',
)


class CoreConnectionSettings:
    ENDPOINT: ClassVar[str] = 'localhost'
    CLUSTER_NAME: ClassVar[str] = 'default'
    MAX_EXECUTION_TIME: ClassVar[int] = 60


DB_CORE_URL = f'clickhouse://datalens:qwerty@{get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).as_pair()}/test_data'

YC_SR_FACTORY = YCServiceRegistryFactory(
    yc_billing_host='fake_host',
    yc_as_endpoint='fake_host',
)

BI_TEST_CONFIG = BiApiTestEnvironmentConfigurationPrivate(
    bi_api_connector_whitelist=['ch_billing_analytics', 'usage_tracking'],
    core_connector_whitelist=['ch_billing_analytics', 'usage_tracking'],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key='_some_test_secret_key_',
)
