from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from dl_connector_starrocks_tests.db.api.base import StarRocksDatasetTestBase


class TestStarRocksDataset(StarRocksDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass
