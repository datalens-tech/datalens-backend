from typing import ClassVar

from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistryFactory
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
    ENDPOINT: ClassVar[str] = "localhost"
    CLUSTER_NAME: ClassVar[str] = "default"
    MAX_EXECUTION_TIME: ClassVar[int] = 60


DB_CORE_URL = f'clickhouse://datalens:qwerty@{get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).as_pair()}/test_data'

# Uid of user yndx-datalens-test, registered at https://passport-test.yandex.ru/
# https://yav.yandex-team.ru/secret/sec-01ep4vyqmaby5an11a395fqfsv
EXT_BLACKBOX_USER_UID = 4057746070

SR_CONNECTION_TABLE_NAME = "sample"
SR_CONNECTION_SETTINGS_PARAMS = dict(
    DB_NAME="test_data",
    HOST=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).host,
    PORT=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).port,
    USERNAME="datalens",
    PASSWORD="qwerty",
    USE_MANAGED_NETWORK=False,
    ALLOWED_TABLES=[],
    SUBSELECT_TEMPLATES=(
        dict(
            title=SR_CONNECTION_TABLE_NAME,
            sql_query="SELECT * FROM uid_table_1 WHERE int_value = :passport_user_id",
        ),
    ),
)

YC_SR_FACTORY = YCServiceRegistryFactory(blackbox_name="Test")
