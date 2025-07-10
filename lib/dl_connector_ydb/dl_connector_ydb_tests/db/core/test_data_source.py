import pytest

from dl_constants.enums import (
    RawSQLLevel,
    UserDataType,
)
from dl_core.data_source_spec.sql import (
    StandardSchemaSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core_testing.testcases.data_source import DefaultDataSourceTestClass

from dl_connector_ydb.core.ydb.constants import (
    SOURCE_TYPE_YDB_SUBSELECT,
    SOURCE_TYPE_YDB_TABLE,
)
from dl_connector_ydb.core.ydb.data_source import (
    YDBSubselectDataSource,
    YDBTableDataSource,
)
from dl_connector_ydb.core.ydb.us_connection import YDBConnection
from dl_connector_ydb_tests.db.config import TABLE_SCHEMA
from dl_connector_ydb_tests.db.core.base import BaseYDBTestClass


class TestYDBTableDataSource(
    BaseYDBTestClass,
    DefaultDataSourceTestClass[
        YDBConnection,
        StandardSchemaSQLDataSourceSpec,
        YDBTableDataSource,
    ],
):
    DSRC_CLS = YDBTableDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> StandardSchemaSQLDataSourceSpec:
        dsrc_spec = StandardSchemaSQLDataSourceSpec(
            source_type=SOURCE_TYPE_YDB_TABLE,
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return [(s[0], s[1]) for s in TABLE_SCHEMA]


class TestYDBSubselectDataSource(
    BaseYDBTestClass,
    DefaultDataSourceTestClass[
        YDBConnection,
        SubselectDataSourceSpec,
        YDBSubselectDataSource,
    ],
):
    DSRC_CLS = YDBSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_YDB_SUBSELECT,
            subsql=f"SELECT * FROM `{sample_table.name}`",
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return [(s[0], s[1]) for s in TABLE_SCHEMA]
