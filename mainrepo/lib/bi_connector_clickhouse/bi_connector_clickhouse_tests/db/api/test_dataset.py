from bi_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from bi_connector_clickhouse_tests.db.api.base import ClickHouseDatasetTestBase


class TestClickHouseDataset(ClickHouseDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass
