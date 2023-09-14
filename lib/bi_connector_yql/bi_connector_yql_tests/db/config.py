from typing import ClassVar

from bi_core_testing.configuration import DefaultCoreTestConfiguration
from bi_testing.containers import get_test_container_hostport

from bi_connector_yql.core.yq.settings import YQConnectorSettings
from bi_connector_yql.formula.constants import YqlDialect as D


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport('us', fallback_port=51911).host,
    port_us_http=get_test_container_hostport('us', fallback_port=51911).port,
    host_us_pg=get_test_container_hostport('pg-us', fallback_port=51910).host,
    port_us_pg_5432=get_test_container_hostport('pg-us', fallback_port=51910).port,
    us_master_token='AC1ofiek8coB',
)


class CoreConnectionSettings:
    SERVICE_ACCOUNT_ID: ClassVar[str] = 'user1'
    FOLDER_ID: ClassVar[str] = 'folder1'
    PASSWORD: ClassVar[str] = 'qweRTY123'


SR_CONNECTION_SETTINGS = YQConnectorSettings(
    HOST='grpc.yandex-query.cloud-preprod.yandex.net',
    PORT=2135,
    DB_NAME='/root/default',
)

_DB_URL = f'yql:///?endpoint={get_test_container_hostport("db-ydb", fallback_port=51900).host}%3A{get_test_container_hostport("db-ydb", fallback_port=51900).port}&database=%2Flocal'
DB_CORE_URL = _DB_URL
DB_CONFIGURATIONS = {
    D.YDB: _DB_URL,
}
