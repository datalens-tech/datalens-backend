from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from dl_connector_mysql_tests.db.api.base import MySQLDatasetTestBase


class TestMySQLDataset(MySQLDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass
