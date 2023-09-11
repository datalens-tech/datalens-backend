import os
from typing import ClassVar

from bi_core_testing.configuration import DefaultCoreTestConfiguration
from bi_testing.containers import get_test_container_hostport
from bi_connector_clickhouse.formula.constants import ClickHouseDialect as D

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration

# Infra settings
SSL_CA_FILENAME = 'marsnet_ca.crt'


def get_clickhouse_ssl_ca_path() -> str:
    return os.path.join(os.path.dirname(__file__), '../../docker-compose/db-clickhouse/ssl', SSL_CA_FILENAME)


def get_clickhouse_ssl_ca() -> str:
    path = get_clickhouse_ssl_ca_path()

    with open(path) as f:
        return f.read()


CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport('us', fallback_port=52211).host,
    port_us_http=get_test_container_hostport('us', fallback_port=52211).port,
    host_us_pg=get_test_container_hostport('pg-us', fallback_port=52210).host,
    port_us_pg_5432=get_test_container_hostport('pg-us', fallback_port=52210).port,
    us_master_token='AC1ofiek8coB',
)


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = 'test_data'
    HOST: ClassVar[str] = get_test_container_hostport('db-clickhouse-22-10', fallback_port=52204).host
    PORT: ClassVar[int] = get_test_container_hostport('db-clickhouse-22-10', fallback_port=52204).port
    USERNAME: ClassVar[str] = 'datalens'
    PASSWORD: ClassVar[str] = 'qwerty'


class CoreSslConnectionSettings:
    DB_NAME: ClassVar[str] = 'test_data'
    # don't use get_test_container_hostport, localhost in needed due to IP usage deprecation in CN
    HOST: ClassVar[str] = 'localhost'
    PORT: ClassVar[int] = 52206
    USERNAME: ClassVar[str] = 'datalens'
    PASSWORD: ClassVar[str] = 'qwerty'


DB_URLS = {
    D.CLICKHOUSE_21_8: f'clickhouse://datalens:qwerty@'
                       f'{get_test_container_hostport("db-clickhouse-21-8", fallback_port=52202).as_pair()}/test_data',
    D.CLICKHOUSE_22_10: f'clickhouse://datalens:qwerty@'
                        f'{get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).as_pair()}/test_data',
}
DB_CORE_URL = DB_URLS[D.CLICKHOUSE_22_10]

BI_TEST_CONFIG = BiApiTestEnvironmentConfiguration(
    bi_api_connector_whitelist=['clickhouse'],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key='_some_test_secret_key_',
)

# don't use get_test_container_hostport, localhost in needed due to IP usage deprecation in CN
DB_CORE_SSL_URL = 'clickhouse://datalens:qwerty@localhost:52206/test_data?protocol=https'
