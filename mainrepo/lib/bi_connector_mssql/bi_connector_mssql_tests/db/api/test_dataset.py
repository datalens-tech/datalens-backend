from bi_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from bi_connector_mssql_tests.db.api.base import MSSQLDatasetTestBase


class TestMSSQLDataset(MSSQLDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass
