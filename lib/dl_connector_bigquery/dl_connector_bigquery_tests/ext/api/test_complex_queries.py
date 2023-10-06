from dl_api_lib_testing.connector.complex_queries import DefaultBasicComplexQueryTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_bigquery_tests.ext.api.base import BigQueryDataApiTestBase


class TestBigQueryBasicComplexQueries(BigQueryDataApiTestBase, DefaultBasicComplexQueryTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultBasicComplexQueryTestSuite.feature_window_functions: "Native window functions are not implemented"
        }
    )
