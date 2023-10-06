from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_bigquery_tests.ext.api.base import BigQueryDataApiTestBase


class TestBigQueryDataResult(BigQueryDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "BigQuery doesn't support arrays",
        }
    )


class TestBigQueryDataGroupBy(BigQueryDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestBigQueryDataRange(BigQueryDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestBigQueryDataDistinct(BigQueryDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestBigQueryDataPreview(BigQueryDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass
