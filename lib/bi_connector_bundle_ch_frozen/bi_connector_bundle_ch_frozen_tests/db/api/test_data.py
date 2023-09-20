from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataResultTestSuite,
)

from bi_connector_bundle_ch_frozen_tests.db.api.base import (
    CHFrozenDataApiSubselectTestBase,
    CHFrozenDataApiTestBase,
)
from dl_testing.regulated_test import RegulatedTestParams


class TestCHFrozenPreview(CHFrozenDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestCHFrozenResult(CHFrozenDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "Frozen connectors doesn't support creating new tables",
        }
    )


class TestCHFrozenSubselectPreview(CHFrozenDataApiSubselectTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestCHFrozenSubselectResult(CHFrozenDataApiSubselectTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "Frozen connectors doesn't support creating new tables",
        }
    )
