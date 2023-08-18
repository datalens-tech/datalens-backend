from bi_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from bi_connector_mysql_tests.db.api.base import MySQLDatasetTestBase


class TestMySQLDataset(MySQLDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass
