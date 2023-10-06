from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from dl_connector_postgresql_tests.db.api.base import PostgreSQLDatasetTestBase


class TestPostgreSQLDataset(PostgreSQLDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass
