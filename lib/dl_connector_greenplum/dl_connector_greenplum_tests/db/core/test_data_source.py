import pytest

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

from dl_connector_greenplum.core.constants import (
    SOURCE_TYPE_GP_SUBSELECT,
    SOURCE_TYPE_GP_TABLE,
)
from dl_connector_greenplum.core.data_source import (
    GreenplumSubselectDataSource,
    GreenplumTableDataSource,
)
from dl_connector_greenplum.core.us_connection import GreenplumConnection
from dl_connector_greenplum_tests.db.config import DASHSQL_QUERY
from dl_connector_greenplum_tests.db.core.base import (
    GP6TestClass,
    GP7TestClass,
)


class TestGP6TableDataSource(
    GP6TestClass,
    DefaultDataSourceTestClass[
        GreenplumConnection,
        StandardSchemaSQLDataSourceSpec,
        GreenplumTableDataSource,
    ],
):
    DSRC_CLS = GreenplumTableDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> StandardSchemaSQLDataSourceSpec:
        dsrc_spec = StandardSchemaSQLDataSourceSpec(
            source_type=SOURCE_TYPE_GP_TABLE,
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestGP6SubselectDataSource(
    GP6TestClass,
    DefaultDataSourceTestClass[
        GreenplumConnection,
        SubselectDataSourceSpec,
        GreenplumSubselectDataSource,
    ],
):
    DSRC_CLS = GreenplumSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_GP_SUBSELECT,
            subsql=f'SELECT * FROM "{sample_table.name}"',
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestGP6SubselectByView(
    GP6TestClass,
    DataSourceTestByViewClass[
        GreenplumConnection,
        SubselectDataSourceSpec,
        GreenplumSubselectDataSource,
    ],
):
    DSRC_CLS = GreenplumSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_GP_SUBSELECT,
            subsql=DASHSQL_QUERY,
        )
        return dsrc_spec


class TestGP7TableDataSource(
    GP7TestClass,
    DefaultDataSourceTestClass[
        GreenplumConnection,
        StandardSchemaSQLDataSourceSpec,
        GreenplumTableDataSource,
    ],
):
    DSRC_CLS = GreenplumTableDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> StandardSchemaSQLDataSourceSpec:
        dsrc_spec = StandardSchemaSQLDataSourceSpec(
            source_type=SOURCE_TYPE_GP_TABLE,
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestGP7SubselectDataSource(
    GP7TestClass,
    DefaultDataSourceTestClass[
        GreenplumConnection,
        SubselectDataSourceSpec,
        GreenplumSubselectDataSource,
    ],
):
    DSRC_CLS = GreenplumSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_GP_SUBSELECT,
            subsql=f'SELECT * FROM "{sample_table.name}"',
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestGP7SubselectByView(
    GP7TestClass,
    DataSourceTestByViewClass[
        GreenplumConnection,
        SubselectDataSourceSpec,
        GreenplumSubselectDataSource,
    ],
):
    DSRC_CLS = GreenplumSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_GP_SUBSELECT,
            subsql=DASHSQL_QUERY,
        )
        return dsrc_spec
