from dl_core_testing.testcases.qe_serializer import DefaultQESerializerTestSuite

from dl_connector_clickhouse.core.clickhouse.testing.exec_factory import ClickHouseExecutorFactory
from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass


class TestClickHouseQESerializer(
    BaseClickHouseTestClass,
    DefaultQESerializerTestSuite[ConnectionClickhouse],
):
    EXECUTOR_FACTORY_CLS = ClickHouseExecutorFactory
