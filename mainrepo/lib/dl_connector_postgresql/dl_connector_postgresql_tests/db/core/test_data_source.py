import pytest

from dl_connector_postgresql.core.postgresql.constants import (
    SOURCE_TYPE_PG_SUBSELECT,
    SOURCE_TYPE_PG_TABLE,
)
from dl_connector_postgresql.core.postgresql.data_source import (
    PostgreSQLDataSource,
    PostgreSQLSubselectDataSource,
)
from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_connector_postgresql_tests.db.config import SUBSELECT_QUERY_FULL
from dl_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass
from dl_constants.enums import (
    RawSQLLevel,
    UserDataType,
)
from dl_core.data_source_spec.sql import (
    StandardSchemaSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.data_source import (
    DataSourceTestByViewClass,
    DefaultDataSourceTestClass,
)


class TestPostgreSQLTableDataSource(
    BasePostgreSQLTestClass,
    DefaultDataSourceTestClass[
        ConnectionPostgreSQL,
        StandardSchemaSQLDataSourceSpec,
        PostgreSQLDataSource,
    ],
):
    DSRC_CLS = PostgreSQLDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> StandardSchemaSQLDataSourceSpec:
        dsrc_spec = StandardSchemaSQLDataSourceSpec(
            source_type=SOURCE_TYPE_PG_TABLE,
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
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

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_PG_SUBSELECT,
            subsql=f'SELECT * FROM "{sample_table.name}"',
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
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

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_PG_SUBSELECT,
            subsql=SUBSELECT_QUERY_FULL,
        )
        return dsrc_spec
