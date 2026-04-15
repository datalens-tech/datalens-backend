import pytest

from dl_constants.enums import (
    RawSQLLevel,
    UserDataType,
)
from dl_core_testing.testcases.data_source import DefaultDataSourceTestClass

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
from dl_connector_snowflake_tests.ext.settings import Settings


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
    def initial_data_source_spec(self, settings: Settings) -> SnowFlakeTableDataSourceSpec:
        dsrc_spec = SnowFlakeTableDataSourceSpec(
            source_type=SOURCE_TYPE_SNOWFLAKE_TABLE,
            table_name=settings.SNOWFLAKE_CONFIG["table_name"],
            db_name=settings.SNOWFLAKE_CONFIG["database"],
            schema_name=settings.SNOWFLAKE_CONFIG["schema"],
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
    def initial_data_source_spec(self, settings: Settings) -> SnowFlakeSubselectDataSourceSpec:
        dsrc_spec = SnowFlakeSubselectDataSourceSpec(
            source_type=SOURCE_TYPE_SNOWFLAKE_SUBSELECT,
            subsql=f"SELECT * FROM {settings.SNOWFLAKE_CONFIG['database']}.{settings.SNOWFLAKE_CONFIG['schema']}.{settings.SNOWFLAKE_CONFIG['table_name']}",
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return SAMPLE_TABLE_SIMPLIFIED_SCHEMA
