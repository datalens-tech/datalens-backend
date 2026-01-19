from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataCacheTestSuite,
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)

from dl_connector_greenplum_tests.db.api.base import (
    GP6DataApiTestBase,
    GP7DataApiTestBase,
)


# GP6


class TestGP6DataResult(GP6DataApiTestBase, DefaultConnectorDataResultTestSuite):
    pass


class TestGP6DataGroupBy(GP6DataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestGP6DataRange(GP6DataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestGP6DataDistinct(GP6DataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestGP6DataPreview(GP6DataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestGP6DataCache(GP6DataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True


# GP7


class TestGP7DataResult(GP7DataApiTestBase, DefaultConnectorDataResultTestSuite):
    pass


class TestGP7DataGroupBy(GP7DataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestGP7DataRange(GP7DataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestGP7DataDistinct(GP7DataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestGP7DataPreview(GP7DataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestGP7DataCache(GP7DataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True
