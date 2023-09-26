import pytest

from dl_connector_snowflake.core.constants import (
    SOURCE_TYPE_SNOWFLAKE_SUBSELECT,
    SOURCE_TYPE_SNOWFLAKE_TABLE,
)
from dl_connector_snowflake.core.data_source import (
    SnowFlakeSubselectDataSource,
    SnowFlakeTableDataSource,
)
from dl_connector_snowflake.core.data_source_spec import (
    SnowFlakeSubselectDataSourceSpec,
    SnowFlakeTableDataSourceSpec,
)
from dl_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake
from dl_connector_snowflake_tests.ext.config import SAMPLE_TABLE_SIMPLIFIED_SCHEMA
from dl_connector_snowflake_tests.ext.core.base import BaseSnowFlakeTestClass
from dl_constants.enums import (
    RawSQLLevel,
    UserDataType,
)
from dl_core_testing.testcases.data_source import DefaultDataSourceTestClass


class TestSnowFlakeTableDataSource(
    BaseSnowFlakeTestClass,
    DefaultDataSourceTestClass[
        ConnectionSQLSnowFlake,
        SnowFlakeTableDataSourceSpec,
        SnowFlakeTableDataSource,
    ],
):
    DSRC_CLS = SnowFlakeTableDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sf_secrets) -> SnowFlakeTableDataSourceSpec:
        dsrc_spec = SnowFlakeTableDataSourceSpec(
            source_type=SOURCE_TYPE_SNOWFLAKE_TABLE,
            table_name=sf_secrets.get_table_name(),
            db_name=sf_secrets.get_database(),
            schema_name=sf_secrets.get_schema(),
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return SAMPLE_TABLE_SIMPLIFIED_SCHEMA


class TestSnowFlakeSubselectDataSoure(
    BaseSnowFlakeTestClass,
    DefaultDataSourceTestClass[
        ConnectionSQLSnowFlake,
        SnowFlakeSubselectDataSourceSpec,
        SnowFlakeSubselectDataSource,
    ],
):
    DSRC_CLS = SnowFlakeSubselectDataSource

    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self, sf_secrets) -> SnowFlakeSubselectDataSourceSpec:
        dsrc_spec = SnowFlakeSubselectDataSourceSpec(
            source_type=SOURCE_TYPE_SNOWFLAKE_SUBSELECT,
            subsql=f"SELECT * FROM {sf_secrets.get_database()}.{sf_secrets.get_schema()}.{sf_secrets.get_table_name()}",
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return SAMPLE_TABLE_SIMPLIFIED_SCHEMA
