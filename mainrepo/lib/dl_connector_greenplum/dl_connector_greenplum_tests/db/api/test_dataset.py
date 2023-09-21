from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite
from dl_connector_greenplum_tests.db.api.base import GreenplumDatasetTestBase


class TestGreenplumDataset(GreenplumDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass
