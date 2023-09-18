from dl_constants.enums import RawSQLLevel

from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport('us', fallback_port=51911).host,
    port_us_http=get_test_container_hostport('us', fallback_port=51911).port,
    host_us_pg=get_test_container_hostport('pg-us', fallback_port=51910).host,
    port_us_pg_5432=get_test_container_hostport('pg-us', fallback_port=51910).port,
    us_master_token='AC1ofiek8coB',
)

DB_CORE_URL = f'clickhouse://datalens:qwerty@{get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).as_pair()}/test_data'

_CONNECTOR_WHITELIST = [
    'ch_frozen_bumpy_roads',
    'ch_frozen_covid',
    'ch_frozen_demo',
    'ch_frozen_dtp',
    'ch_frozen_gkh',
    'ch_frozen_samples',
    'ch_frozen_transparency',
    'ch_frozen_weather',
    'ch_frozen_horeca',
]
BI_TEST_CONFIG = BiApiTestEnvironmentConfiguration(
    bi_api_connector_whitelist=_CONNECTOR_WHITELIST,
    core_connector_whitelist=_CONNECTOR_WHITELIST,
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key='_some_test_secret_key_',
)

DB_NAME = 'test_data'
TABLE_NAME = 'sample'
SUBSELECT_QUERY = f'select * from {TABLE_NAME} limit 10'
CONNECTION_PARAMS = dict(
    secure=False,
    db_name=DB_NAME,
    host=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).host,
    port=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).port,
    username='datalens',
    password='qwerty',
)
SR_CONNECTION_SETTINGS_PARAMS = {key.upper(): elem for key, elem in CONNECTION_PARAMS.items()} | dict(
    USE_MANAGED_NETWORK=False,
    SUBSELECT_TEMPLATES=(
        {
            'title': 'subselect_1',
            'sql_query': SUBSELECT_QUERY,
        },
    ),
    PASS_DB_QUERY_TO_USER=True,
    RAW_SQL_LEVEL=RawSQLLevel.dashsql,
)
