import pytest

from dl_core_testing.database import DbTable
from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_bigquery.core.constants import SOURCE_TYPE_BIGQUERY_TABLE
from dl_connector_bigquery.core.us_connection import ConnectionSQLBigQuery
from dl_connector_bigquery_tests.ext.core.base import BaseBigQueryTestClass


class TestBigQueryDataset(BaseBigQueryTestClass, DefaultDatasetTestSuite[ConnectionSQLBigQuery]):
    source_type = SOURCE_TYPE_BIGQUERY_TABLE

    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: "",  # TODO: FIXME
        },
    )

    @pytest.fixture(scope="function")
    def dsrc_params(self, dataset_table: DbTable) -> dict:
        return dict(
            project_id=dataset_table.db.name,
            dataset_name=dataset_table.schema,
            table_name=dataset_table.name,
        )
