from bi_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from bi_connector_bigquery_tests.ext.api.base import BigQueryDatasetTestBase


class TestBigQueryDataset(BigQueryDatasetTestBase, DefaultConnectorDatasetTestSuite):
    pass
