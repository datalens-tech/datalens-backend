from typing import ClassVar

from bi_core_testing.configuration import DefaultCoreTestConfiguration
from bi_testing.containers import get_test_container_hostport

from bi_connector_bundle_partners.base.core.settings import PartnerKeys


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport('us', fallback_port=51911).host,
    port_us_http=get_test_container_hostport('us', fallback_port=51911).port,
    host_us_pg=get_test_container_hostport('pg-us', fallback_port=51910).host,
    port_us_pg_5432=get_test_container_hostport('pg-us', fallback_port=51910).port,
    us_master_token='AC1ofiek8coB',
)

DB_CORE_URL = f'clickhouse://datalens:qwerty@{get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).as_pair()}/test_data'

DB_NAME = 'test_data'
TABLE_NAME = 'sample'


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = DB_NAME
    ENDPOINT: ClassVar[str] = 'localhost'
    CLUSTER_NAME: ClassVar[str] = 'default'
    MAX_EXECUTION_TIME: ClassVar[int] = 60


SR_CONNECTION_SETTINGS_PARAMS = dict(
    SECURE=False,
    HOST=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204, original_port=8123).host,
    PORT=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204, original_port=8123).port,
    USERNAME='datalens',
    PASSWORD='qwerty',
    USE_MANAGED_NETWORK=True,
    PARTNER_KEYS=PartnerKeys(dl_private={}, partner_public={}),
)
