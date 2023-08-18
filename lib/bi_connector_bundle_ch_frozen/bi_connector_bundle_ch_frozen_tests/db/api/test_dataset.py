from bi_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from bi_connector_bundle_ch_frozen_tests.db.api.base import CHFrozenDatasetTestBase, CHFrozenDatasetSubselectTestBase


class TestCHFrozenDataset(CHFrozenDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass


class TestCHFrozenDatasetSubselect(CHFrozenDatasetSubselectTestBase, DefaultConnectorDatasetTestSuite):
    pass
