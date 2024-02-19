from dl_core_testing.testcases.typed_query import DefaultTypedQueryTestSuite
from dl_core_tests.db.base import DefaultCoreTestClass

from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse


class TestClickHouseConnection(
    DefaultCoreTestClass,
    DefaultTypedQueryTestSuite[ConnectionClickhouse],
):
    pass
