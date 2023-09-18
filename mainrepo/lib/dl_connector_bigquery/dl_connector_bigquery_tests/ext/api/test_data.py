from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_connector_bigquery_tests.ext.api.base import BigQueryDataApiTestBase


class TestBigQueryDataResult(BigQueryDataApiTestBase, DefaultConnectorDataResultTestSuite):
    pass


class TestBigQueryDataGroupBy(BigQueryDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestBigQueryDataRange(BigQueryDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestBigQueryDataDistinct(BigQueryDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass
