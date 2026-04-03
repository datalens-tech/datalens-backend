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
        mark_tests_skipped={
            DefaultConnectorDataResultTestSuite.test_basic_result: "TODO: BI-7170 formula definitions not implemented",
            DefaultConnectorDataResultTestSuite.test_duplicated_expressions: "TODO: BI-7170 formula definitions not implemented",
            DefaultConnectorDataResultTestSuite.test_dates: "TODO: BI-7170 formula definitions not implemented",
            DefaultConnectorDataResultTestSuite.test_get_result_with_formula_in_where: "TODO: BI-7170 formula definitions not implemented",
            DefaultConnectorDataResultTestSuite.test_get_result_with_string_filter_operations_for_numbers: "TODO: BI-7170 formula definitions not implemented",
        },
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "TODO: BI-7170 formula definitions not implemented",
        },
    )


class TestStarRocksDataGroupBy(StarRocksDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorDataGroupByFormulaTestSuite.test_complex_result: "TODO: BI-7170 formula definitions not implemented",
        },
    )


class TestStarRocksDataRange(StarRocksDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorDataRangeTestSuite.test_basic_range: "TODO: BI-7170 formula definitions not implemented",
        },
    )


class TestStarRocksDataDistinct(StarRocksDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorDataDistinctTestSuite.test_date_filter_distinct: "TODO: BI-7170 formula definitions not implemented",
        },
    )


class TestStarRocksDataPreview(StarRocksDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestStarRocksDataCache(StarRocksDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorDataCacheTestSuite.test_cache_with_filter_with_constants: "TODO: BI-7170 formula definitions not implemented",
        },
    )
