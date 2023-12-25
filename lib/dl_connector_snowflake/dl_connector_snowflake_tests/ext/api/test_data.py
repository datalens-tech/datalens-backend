from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataCacheTestSuite,
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_snowflake_tests.ext.api.base import SnowFlakeDataApiTestBase


class TestSnowFlakeDataResult(SnowFlakeDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "SnowFlake doesn't support arrays",
        }
    )


class TestSnowFlakeDataRange(SnowFlakeDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestSnowFlakeDataDistinct(SnowFlakeDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestSnowFlakeDataGroupBy(SnowFlakeDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestSnowFlakeDataPreview(SnowFlakeDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestSnowFlakeDataCache(SnowFlakeDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True
