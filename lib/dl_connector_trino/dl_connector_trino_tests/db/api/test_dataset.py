from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from dl_connector_trino_tests.db.api.base import TrinoDatasetTestBase


class TestTrinoDataset(TrinoDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass
