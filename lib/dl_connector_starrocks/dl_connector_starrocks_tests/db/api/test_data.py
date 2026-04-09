from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataCacheTestSuite,
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_starrocks_tests.db.api.base import StarRocksDataApiTestBase


class TestStarRocksDataResult(StarRocksDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "TODO: BI-7172 formula definitions not implemented",
        },
    )


class TestStarRocksDataGroupBy(StarRocksDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestStarRocksDataRange(StarRocksDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestStarRocksDataDistinct(StarRocksDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestStarRocksDataPreview(StarRocksDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestStarRocksDataCache(StarRocksDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True
