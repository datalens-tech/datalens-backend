import pytest

from dl_constants.enums import (
    RawSQLLevel,
    UserDataType,
)
from dl_core.data_source_spec.sql import (
    StandardSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core_testing.database import DbTable
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.data_source import DefaultDataSourceTestClass

from dl_connector_trino.core.constants import (
    SOURCE_TYPE_TRINO_SUBSELECT,
    SOURCE_TYPE_TRINO_TABLE,
)
from dl_connector_trino.core.data_source import (
    TrinoDataSource,
    TrinoSubselectDataSource,
)
from dl_connector_trino.core.us_connection import ConnectionTrino
from dl_connector_trino_tests.db.core.base import BaseTrinoTestClass
import dl_connector_trino_tests.db.config as test_config


class TestTrinoTableDataSource(
    BaseTrinoTestClass,
    DefaultDataSourceTestClass[
        ConnectionTrino,
        StandardSQLDataSourceSpec,
        TrinoDataSource,
    ],
):
    DSRC_CLS = TrinoDataSource

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_URL_MEMORY_CATALOG

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table: DbTable) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_TRINO_TABLE,
            db_name=sample_table.db.name,
            # schema_name=?
            table_name=sample_table.name,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)


class TestTrinoSubselectDataSource(
    BaseTrinoTestClass,
    DefaultDataSourceTestClass[
        ConnectionTrino,
        SubselectDataSourceSpec,
        TrinoSubselectDataSource,
    ],
):
    DSRC_CLS = TrinoSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sample_table: DbTable) -> SubselectDataSourceSpec:
        dsrc_spec = SubselectDataSourceSpec(
            source_type=SOURCE_TYPE_TRINO_SUBSELECT,
            subsql=f'SELECT * FROM "{sample_table.name}"',
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)
