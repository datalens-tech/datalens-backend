from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataCacheTestSuite,
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)

from dl_connector_starrocks_tests.db.api.base import StarRocksDataApiTestBase


class TestStarRocksDataResult(StarRocksDataApiTestBase, DefaultConnectorDataResultTestSuite):  # type: ignore  # 2024-01-30 # TODO: fix
    pass


class TestStarRocksDataGroupBy(StarRocksDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):  # type: ignore  # 2024-01-30 # TODO: fix
    pass


class TestStarRocksDataRange(StarRocksDataApiTestBase, DefaultConnectorDataRangeTestSuite):  # type: ignore  # 2024-01-30 # TODO: fix
    pass


class TestStarRocksDataDistinct(StarRocksDataApiTestBase, DefaultConnectorDataDistinctTestSuite):  # type: ignore  # 2024-01-30 # TODO: fix
    pass


class TestStarRocksDataPreview(StarRocksDataApiTestBase, DefaultConnectorDataPreviewTestSuite):  # type: ignore  # 2024-01-30 # TODO: fix
    pass


class TestStarRocksDataCache(StarRocksDataApiTestBase, DefaultConnectorDataCacheTestSuite):  # type: ignore  # 2024-01-30 # TODO: fix
    pass
