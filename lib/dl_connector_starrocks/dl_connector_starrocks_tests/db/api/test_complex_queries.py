from dl_api_lib_testing.connector.complex_queries import DefaultBasicComplexQueryTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_starrocks_tests.db.api.base import StarRocksDataApiTestBase


class TestStarRocksBasicComplexQueries(StarRocksDataApiTestBase, DefaultBasicComplexQueryTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultBasicComplexQueryTestSuite.test_window_functions: "TODO: BI-7172 formula definitions not implemented",
        },
    )
