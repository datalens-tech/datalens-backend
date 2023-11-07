import pytest

from dl_constants.enums import (
    RawSQLLevel,
    UserDataType,
)
from dl_core.data_source_spec.sql import (
    StandardSchemaSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core.db import SchemaColumn
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.data_source import (
    DataSourceTestByViewClass,
    DefaultDataSourceTestClass,
)

from dl_connector_mssql.core.constants import (
    SOURCE_TYPE_MSSQL_SUBSELECT,
    SOURCE_TYPE_MSSQL_TABLE,
)
from dl_connector_mssql.core.data_source import (
    MSSQLDataSource,
    MSSQLSubselectDataSource,
)
from dl_connector_mssql.core.us_connection import ConnectionMSSQL
from dl_connector_mssql_tests.db.config import SUBSELECT_QUERY_FULL
from dl_connector_mssql_tests.db.core.base import BaseMSSQLTestClass


class TestMSSQLTableDataSource(
    BaseMSSQLTestClass,
    DefaultDataSourceTestClass[
        ConnectionMSSQL,
        StandardSchemaSQLDataSourceSpec,
        MSSQLDataSource,
    ],
):
    DSRC_CLS = MSSQLDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> StandardSchemaSQLDataSourceSpec:
        dsrc_spec = StandardSchemaSQLDataSourceSpec(
            source_type=SOURCE_TYPE_MSSQL_TABLE,
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestMSSQLSubselectDataSource(
    BaseMSSQLTestClass,
    DefaultDataSourceTestClass[
        ConnectionMSSQL,
        SubselectDataSourceSpec,
        MSSQLSubselectDataSource,
    ],
):
    DSRC_CLS = MSSQLSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_MSSQL_SUBSELECT,
            subsql=f'SELECT * FROM "{sample_table.name}"',
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        remapped_types = {UserDataType.date: UserDataType.string}
        expected_schema = [
            # MSSQL cannot identify dates in sub-queries correctly
            (name, remapped_types.get(user_type, user_type))
            for name, user_type in TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema
        ]
        return expected_schema


class TestMSSQLSubselectByView(
    BaseMSSQLTestClass,
    DataSourceTestByViewClass[
        ConnectionMSSQL,
        SubselectDataSourceSpec,
        MSSQLSubselectDataSource,
    ],
):
    DSRC_CLS = MSSQLSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="session")
    def initial_data_source_spec(self) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_MSSQL_SUBSELECT,
            subsql=SUBSELECT_QUERY_FULL,
        )
        return dsrc_spec

    def postprocess_view_schema_column(self, schema_col: SchemaColumn) -> SchemaColumn:
        # MSSQL subselect schema does not use a cursor-based types
        # (it uses `sp_describe_first_result_set`)
        # but it still manages to make some critical failures,
        # representing date/datetime type as a string type.
        if schema_col.native_type.name in ("date", "datetime2", "datetimeoffset"):
            schema_col = schema_col.clone(
                user_type=UserDataType.string, native_type=schema_col.native_type.clone(name="nvarchar")
            )
        return schema_col
