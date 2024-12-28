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
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectorDataDistinctTestSuite.test_date_filter_distinct: "Requires db fixture, but it is not defined for Snowflake",
        }
    )


class TestSnowFlakeDataGroupBy(SnowFlakeDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestSnowFlakeDataPreview(SnowFlakeDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestSnowFlakeDataCache(SnowFlakeDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True

    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectorDataCacheTestSuite.test_cache_with_filter_with_constants: "Requires db fixture, but it is not defined for Snowflake",
        }
    )
