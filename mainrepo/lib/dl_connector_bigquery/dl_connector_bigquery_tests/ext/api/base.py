import pytest

from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_connector_bigquery.core.constants import (
    CONNECTION_TYPE_BIGQUERY,
    SOURCE_TYPE_BIGQUERY_TABLE,
)
from dl_connector_bigquery_tests.ext.config import BI_TEST_CONFIG
from dl_connector_bigquery_tests.ext.core.base import BaseBigQueryTestClass


class BigQueryConnectionTestBase(BaseBigQueryTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_BIGQUERY
    bi_compeng_pg_on = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> BiApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self, bq_secrets) -> dict:
        return dict(
            project_id=bq_secrets.get_project_id(),
            credentials=bq_secrets.get_creds(),
        )


class BigQueryDatasetTestBase(BigQueryConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table) -> dict:
        return dict(
            source_type=SOURCE_TYPE_BIGQUERY_TABLE.name,
            parameters=dict(
                dataset_name=sample_table.schema,
                table_name=sample_table.name,
            ),
        )


class BigQueryDataApiTestBase(BigQueryDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False
