import pytest

from dl_constants.enums import (
    BIType,
    RawSQLLevel,
)
from dl_core.data_source_spec.sql import (
    StandardSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core.db import SchemaColumn
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.data_source import (
    DataSourceTestByViewClass,
    DefaultDataSourceTestClass,
)

from bi_connector_mysql.core.constants import (
    SOURCE_TYPE_MYSQL_SUBSELECT,
    SOURCE_TYPE_MYSQL_TABLE,
)
from bi_connector_mysql.core.data_source import (
    MySQLDataSource,
    MySQLSubselectDataSource,
)
from bi_connector_mysql.core.us_connection import ConnectionMySQL
from bi_connector_mysql_tests.db.config import SUBSELECT_QUERY_FULL
from bi_connector_mysql_tests.db.core.base import BaseMySQLTestClass


class TestMySQLTableDataSource(
    BaseMySQLTestClass,
    DefaultDataSourceTestClass[
        ConnectionMySQL,
        StandardSQLDataSourceSpec,
        MySQLDataSource,
    ],
):
    DSRC_CLS = MySQLDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_MYSQL_TABLE,
            db_name=sample_table.db.name,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, BIType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestMySQLSubselectDataSource(
    BaseMySQLTestClass,
    DefaultDataSourceTestClass[
        ConnectionMySQL,
        SubselectDataSourceSpec,
        MySQLSubselectDataSource,
    ],
):
    DSRC_CLS = MySQLSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_MYSQL_SUBSELECT,
            subsql=f"SELECT * FROM `{sample_table.name}`",
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, BIType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestMySQLSubselectByView(
    BaseMySQLTestClass,
    DataSourceTestByViewClass[
        ConnectionMySQL,
        SubselectDataSourceSpec,
        MySQLSubselectDataSource,
    ],
):
    DSRC_CLS = MySQLSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_MYSQL_SUBSELECT,
            subsql=SUBSELECT_QUERY_FULL,
        )
        return dsrc_spec

    def postprocess_view_schema_column(self, schema_col: SchemaColumn) -> SchemaColumn:
        nt_map = {
            # view-schema type -> cursor-schema type
            "varchar": "text",
            "varbinary": "text",
        }
        if schema_col.native_type.name in nt_map:
            schema_col = schema_col.clone(
                native_type=schema_col.native_type.clone(name=nt_map[schema_col.native_type.name])
            )
        return schema_col
