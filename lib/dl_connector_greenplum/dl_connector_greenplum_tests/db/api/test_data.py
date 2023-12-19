from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataCacheTestSuite,
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)

from dl_connector_greenplum_tests.db.api.base import GreenplumDataApiTestBase


class TestGreenplumDataResult(GreenplumDataApiTestBase, DefaultConnectorDataResultTestSuite):
    pass


class TestGreenplumDataGroupBy(GreenplumDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestGreenplumDataRange(GreenplumDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestGreenplumDataDistinct(GreenplumDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestGreenplumDataPreview(GreenplumDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestGreenplumDataCache(GreenplumDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True
