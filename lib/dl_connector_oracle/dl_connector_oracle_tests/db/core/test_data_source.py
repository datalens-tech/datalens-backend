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

from dl_connector_oracle.core.constants import (
    SOURCE_TYPE_ORACLE_SUBSELECT,
    SOURCE_TYPE_ORACLE_TABLE,
)
from dl_connector_oracle.core.data_source import (
    OracleDataSource,
    OracleSubselectDataSource,
)
from dl_connector_oracle.core.us_connection import ConnectionSQLOracle
from dl_connector_oracle_tests.db.config import SUBSELECT_QUERY_FULL
from dl_connector_oracle_tests.db.core.base import BaseOracleTestClass


def _update_utype_for_oracle(user_type: UserDataType) -> UserDataType:
    if user_type == UserDataType.date:
        return UserDataType.genericdatetime
    return user_type


SAMPLE_TABLE_SCHEMA_SUPERSTORE_ORACLIZED = [
    (name, _update_utype_for_oracle(user_type)) for name, user_type in TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema
]


class TestOracleTableDataSource(
    BaseOracleTestClass,
    DefaultDataSourceTestClass[
        ConnectionSQLOracle,
        StandardSchemaSQLDataSourceSpec,
        OracleDataSource,
    ],
):
    DSRC_CLS = OracleDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> StandardSchemaSQLDataSourceSpec:
        dsrc_spec = StandardSchemaSQLDataSourceSpec(
            source_type=SOURCE_TYPE_ORACLE_TABLE,
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(SAMPLE_TABLE_SCHEMA_SUPERSTORE_ORACLIZED)


class TestOracleSubselectDataSource(
    BaseOracleTestClass,
    DefaultDataSourceTestClass[
        ConnectionSQLOracle,
        SubselectDataSourceSpec,
        OracleSubselectDataSource,
    ],
):
    DSRC_CLS = OracleSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_ORACLE_SUBSELECT,
            subsql=f'SELECT * FROM "{sample_table.name}"',
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(SAMPLE_TABLE_SCHEMA_SUPERSTORE_ORACLIZED)


class TestOracleSubselectByView(
    BaseOracleTestClass,
    DataSourceTestByViewClass[
        ConnectionSQLOracle,
        SubselectDataSourceSpec,
        OracleSubselectDataSource,
    ],
):
    DSRC_CLS = OracleSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_ORACLE_SUBSELECT,
            subsql=SUBSELECT_QUERY_FULL,
        )
        return dsrc_spec

    def postprocess_view_schema(
        self, view_schema: list[SchemaColumn], cursor_schema: list[SchemaColumn]
    ) -> list[SchemaColumn]:
        result = super().postprocess_view_schema(view_schema, cursor_schema=cursor_schema)
        if len(view_schema) != len(cursor_schema):
            return result

        # Actual result seems to depend on the cx_Oracle version too much,
        # and DL uses these equivalently anyway.
        tnames = ("binary_double", "binary_float")
        for idx, schema_col in enumerate(result):
            if schema_col.native_type.name in tnames:
                cs_name = cursor_schema[idx].native_type.name
                if cs_name in tnames:
                    schema_col = schema_col.clone(native_type=schema_col.native_type.clone(name=cs_name))
                    result[idx] = schema_col
        return result
