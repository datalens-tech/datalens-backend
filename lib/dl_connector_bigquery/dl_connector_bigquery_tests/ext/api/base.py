import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase

from dl_connector_bigquery.core.constants import (
    CONNECTION_TYPE_BIGQUERY,
    SOURCE_TYPE_BIGQUERY_TABLE,
)
from dl_connector_bigquery_tests.ext.config import API_TEST_CONFIG
from dl_connector_bigquery_tests.ext.core.base import BaseBigQueryTestClass
from dl_connector_bigquery_tests.ext.settings import Settings


class BigQueryConnectionTestBase(BaseBigQueryTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_BIGQUERY
    compeng_enabled = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self, settings: Settings) -> dict:
        return dict(
            project_id=settings.BIGQUERY_CONFIG["project_id"],
            credentials=settings.BIGQUERY_CREDS,
        )


class BigQueryConnectionTestMalformedCreds(BigQueryConnectionTestBase):
    @pytest.fixture(scope="class")
    def connection_params(self, settings: Settings) -> dict:
        return dict(
            project_id=settings.BIGQUERY_CONFIG["project_id"],
            credentials=settings.BIGQUERY_CREDS + "asdf",
        )


class BigQueryConnectionTestBadProjectId(BigQueryConnectionTestBase):
    @pytest.fixture(scope="class")
    def connection_params(self, settings: Settings) -> dict:
        return dict(
            project_id=settings.BIGQUERY_CONFIG["project_id"] + "123",
            credentials=settings.BIGQUERY_CREDS,
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
    mutation_caches_enabled = False
