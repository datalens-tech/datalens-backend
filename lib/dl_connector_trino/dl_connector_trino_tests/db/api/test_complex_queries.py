from dl_api_lib_testing.connector.complex_queries import DefaultBasicComplexQueryTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_trino_tests.db.api.base import TrinoDataApiTestBase


class TestTrinoBasicComplexQueries(TrinoDataApiTestBase, DefaultBasicComplexQueryTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultBasicComplexQueryTestSuite.test_month_ago_for_shorter_month: "BI-6239",
            DefaultBasicComplexQueryTestSuite.test_total_lod_2: "BI-6239",
            DefaultBasicComplexQueryTestSuite.test_lod_in_order_by: "BI-6239",
        }
    )
