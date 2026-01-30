import pytest

from dl_constants.enums import UserDataType
from dl_core.data_source_spec.sql import (
    StandardSchemaSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.data_source import DefaultDataSourceTestClass

from dl_connector_starrocks.core.constants import (
    SOURCE_TYPE_STARROCKS_SUBSELECT,
    SOURCE_TYPE_STARROCKS_TABLE,
)
from dl_connector_starrocks.core.data_source import (
    StarRocksDataSource,
    StarRocksSubselectDataSource,
)
from dl_connector_starrocks.core.us_connection import ConnectionStarRocks
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


class TestStarRocksTableDataSource(
    BaseStarRocksTestClass,
    DefaultDataSourceTestClass[
        ConnectionStarRocks,
        StandardSchemaSQLDataSourceSpec,
        StarRocksDataSource,
    ],
):
    DSRC_CLS = StarRocksDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> StandardSchemaSQLDataSourceSpec:  # type: ignore  # 2024-01-30 # TODO: fix
        dsrc_spec = StandardSchemaSQLDataSourceSpec(
            source_type=SOURCE_TYPE_STARROCKS_TABLE,
            db_name=sample_table.db.name,
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestStarRocksSubselectDataSource(
    BaseStarRocksTestClass,
    DefaultDataSourceTestClass[
        ConnectionStarRocks,
        SubselectDataSourceSpec,
        StarRocksSubselectDataSource,
    ],
):
    DSRC_CLS = StarRocksSubselectDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table) -> SubselectDataSourceSpec:  # type: ignore  # 2024-01-30 # TODO: fix
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_STARROCKS_SUBSELECT,
            subsql=f"SELECT * FROM {sample_table.name}",
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)
