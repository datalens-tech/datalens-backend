from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataCacheTestSuite,
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)

from dl_connector_clickhouse_tests.db.api.base import ClickHouseDataApiTestBase


class TestClickHouseDataResult(ClickHouseDataApiTestBase, DefaultConnectorDataResultTestSuite):
    pass


class TestClickHouseDataGroupBy(ClickHouseDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestClickHouseDataRange(ClickHouseDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestClickHouseDataDistinct(ClickHouseDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestClickHouseDataPreview(ClickHouseDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestClickHouseDataCache(ClickHouseDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True
