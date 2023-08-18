from bi_core.connectors.clickhouse.constants import SOURCE_TYPE_CH_TABLE
from bi_core.connectors.clickhouse.us_connection import ConnectionClickhouse

from bi_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass

from bi_core_testing.testcases.dataset import DefaultDatasetTestSuite


class TestClickHouseDataset(BaseClickHouseTestClass, DefaultDatasetTestSuite[ConnectionClickhouse]):
    source_type = SOURCE_TYPE_CH_TABLE
