import pytest

from dl_constants.enums import (
    BIType,
    RawSQLLevel,
)
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.data_source import DefaultDataSourceTestClass

from dl_connector_bigquery.core.constants import (
    SOURCE_TYPE_BIGQUERY_SUBSELECT,
    SOURCE_TYPE_BIGQUERY_TABLE,
)
from dl_connector_bigquery.core.data_source import (
    BigQuerySubselectDataSource,
    BigQueryTableDataSource,
)
from dl_connector_bigquery.core.data_source_spec import (
    BigQuerySubselectDataSourceSpec,
    BigQueryTableDataSourceSpec,
)
from dl_connector_bigquery.core.us_connection import ConnectionSQLBigQuery
from dl_connector_bigquery_tests.ext.core.base import BaseBigQueryTestClass


class TestBigQueryTableDataSource(
    BaseBigQueryTestClass,
    DefaultDataSourceTestClass[
        ConnectionSQLBigQuery,
        BigQueryTableDataSourceSpec,
        BigQueryTableDataSource,
    ],
):
    DSRC_CLS = BigQueryTableDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> BigQueryTableDataSourceSpec:
        dsrc_spec = BigQueryTableDataSourceSpec(
            source_type=SOURCE_TYPE_BIGQUERY_TABLE,
            dataset_name=sample_table.schema,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, BIType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestBigQuerySubselectDataSource(
    BaseBigQueryTestClass,
    DefaultDataSourceTestClass[
        ConnectionSQLBigQuery,
        BigQuerySubselectDataSourceSpec,
        BigQuerySubselectDataSource,
    ],
):
    DSRC_CLS = BigQuerySubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> BigQuerySubselectDataSourceSpec:
        dsrc_spec = BigQuerySubselectDataSourceSpec(
            source_type=SOURCE_TYPE_BIGQUERY_SUBSELECT,
            subsql=f"SELECT * FROM {sample_table.schema}.{sample_table.name}",
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, BIType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)
