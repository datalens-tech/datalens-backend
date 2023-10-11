from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from dl_connector_oracle_tests.db.api.base import OracleDatasetTestBase


class TestOracleDataset(OracleDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass
