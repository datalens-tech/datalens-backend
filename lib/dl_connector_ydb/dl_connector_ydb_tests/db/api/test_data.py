from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataCacheTestSuite,
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_ydb_tests.db.api.base import YDBDataApiTestBase


class TestYDBDataResult(YDBDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectorDataResultTestSuite.test_array_contains_field: "TODO: No array support",
            DefaultConnectorDataResultTestSuite.test_array_contains_filter: "TODO: No array support",
            DefaultConnectorDataResultTestSuite.test_array_not_contains_filter: "TODO: No array support",
        },
    )


class TestYDBDataGroupBy(YDBDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestYDBDataRange(YDBDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestYDBDataDistinct(YDBDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectorDataDistinctTestSuite.test_date_filter_distinct: "FIXME: KeyError: (SourceBackendType('YDB'), GenericNativeType(name='date'))",
        },
    )


class TestYDBDataPreview(YDBDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestYDBDataCache(YDBDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectorDataCacheTestSuite.test_cache_with_filter_with_constants: "FIXME: KeyError: (SourceBackendType('YDB'), GenericNativeType(name='text'))",
        },
    )
    data_caches_enabled = True
