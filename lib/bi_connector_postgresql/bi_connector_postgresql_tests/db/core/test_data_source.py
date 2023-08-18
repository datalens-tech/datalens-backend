import pytest

from bi_constants.enums import BIType, CreateDSFrom, RawSQLLevel

from bi_core.data_source_spec.sql import StandardSchemaSQLDataSourceSpec, SubselectDataSourceSpec

from bi_core_testing.testcases.data_source import DefaultDataSourceTestClass, DataSourceTestByViewClass
from bi_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE

from bi_connector_postgresql.core.postgresql.data_source import PostgreSQLDataSource, PostgreSQLSubselectDataSource
from bi_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL

from bi_connector_postgresql_tests.db.config import SUBSELECT_QUERY_FULL
from bi_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass


class TestPostgreSQLTableDataSource(
    BasePostgreSQLTestClass,
    DefaultDataSourceTestClass[
        ConnectionPostgreSQL,
        StandardSchemaSQLDataSourceSpec,
        PostgreSQLDataSource,
    ],
):
    DSRC_CLS = PostgreSQLDataSource

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self, sample_table) -> StandardSchemaSQLDataSourceSpec:
        dsrc_spec = StandardSchemaSQLDataSourceSpec(
            source_type=CreateDSFrom.PG_TABLE,
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, BIType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestPostgreSQLSubselectDataSource(
        BasePostgreSQLTestClass,
        DefaultDataSourceTestClass[
            ConnectionPostgreSQL,
            SubselectDataSourceSpec,
            PostgreSQLSubselectDataSource,
        ],
):
    DSRC_CLS = PostgreSQLSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self, sample_table) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=CreateDSFrom.PG_SUBSELECT,
            subsql=f'SELECT * FROM "{sample_table.name}"',
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, BIType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestPostgreSQLSubselectByView(
        BasePostgreSQLTestClass,
        DataSourceTestByViewClass[
            ConnectionPostgreSQL,
            SubselectDataSourceSpec,
            PostgreSQLSubselectDataSource,
        ],
):
    DSRC_CLS = PostgreSQLSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=CreateDSFrom.PG_SUBSELECT,
            subsql=SUBSELECT_QUERY_FULL,
        )
        return dsrc_spec
