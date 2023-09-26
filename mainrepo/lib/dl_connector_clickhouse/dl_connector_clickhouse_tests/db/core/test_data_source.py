import pytest

from dl_connector_clickhouse.core.clickhouse.constants import (
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)
from dl_connector_clickhouse.core.clickhouse.data_source import (
    ClickHouseDataSource,
    ClickHouseSubselectDataSource,
)
from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass
from dl_constants.enums import (
    RawSQLLevel,
    UserDataType,
)
from dl_core.data_source_spec.sql import (
    StandardSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.data_source import DefaultDataSourceTestClass


class TestClickHouseTableDataSource(
    BaseClickHouseTestClass,
    DefaultDataSourceTestClass[
        ConnectionClickhouse,
        StandardSQLDataSourceSpec,
        ClickHouseDataSource,
    ],
):
    DSRC_CLS = ClickHouseDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_CH_TABLE,
            db_name=sample_table.db.name,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestClickHouseSubselectDataSource(
    BaseClickHouseTestClass,
    DefaultDataSourceTestClass[
        ConnectionClickhouse,
        SubselectDataSourceSpec,
        ClickHouseSubselectDataSource,
    ],
):
    DSRC_CLS = ClickHouseSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_CH_SUBSELECT,
            subsql=f'SELECT * FROM "{sample_table.name}"',
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)
