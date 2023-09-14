from bi_core_testing.testcases.dataset import DefaultDatasetTestSuite

from bi_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE
from bi_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from bi_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass


class TestClickHouseDataset(BaseClickHouseTestClass, DefaultDatasetTestSuite[ConnectionClickhouse]):
    source_type = SOURCE_TYPE_CH_TABLE
