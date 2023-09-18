from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataResultTestSuite,
)

from bi_connector_bundle_ch_frozen_tests.db.api.base import (
    CHFrozenDataApiSubselectTestBase,
    CHFrozenDataApiTestBase,
)


class TestCHFrozenPreview(CHFrozenDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestCHFrozenResult(CHFrozenDataApiTestBase, DefaultConnectorDataResultTestSuite):
    pass


class TestCHFrozenSubselectPreview(CHFrozenDataApiSubselectTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestCHFrozenSubselectResult(CHFrozenDataApiSubselectTestBase, DefaultConnectorDataResultTestSuite):
    pass
