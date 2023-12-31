from dl_api_lib_testing.connector.complex_queries import DefaultBasicComplexQueryTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_mssql_tests.db.api.base import MSSQLDataApiTestBase


class TestMSSQLBasicComplexQueries(MSSQLDataApiTestBase, DefaultBasicComplexQueryTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultBasicComplexQueryTestSuite.feature_window_functions: "Native window functions are not implemented"
        }
    )
