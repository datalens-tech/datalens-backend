from dl_api_lib_testing.connector.complex_queries import DefaultBasicComplexQueryTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_starrocks_tests.db.api.base import StarRocksDataApiTestBase


class TestStarRocksBasicComplexQueries(StarRocksDataApiTestBase, DefaultBasicComplexQueryTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultBasicComplexQueryTestSuite.test_lod_for_native_functions: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_window_functions: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_ago_any_db: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_triple_ago_any_db: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_ago_any_db_multisource: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_nested_ago: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_month_ago_for_shorter_month: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_lod_fixed_single_dim_in_two_dim_query: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_null_dimensions: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_total_lod: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_total_lod_2: "TODO: BI-7172 formula definitions not implemented",
            DefaultBasicComplexQueryTestSuite.test_lod_in_order_by: "TODO: BI-7172 formula definitions not implemented",
        },
    )
