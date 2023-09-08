import pytest

from bi_constants.enums import BIType

from bi_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from bi_core_testing.testcases.data_source import DefaultDataSourceTestClass
from bi_connector_chyt_internal.core.us_connection import ConnectionCHYTInternalToken, ConnectionCHYTUserAuth

from bi_connector_chyt.core.data_source_spec import CHYTSubselectDataSourceSpec
from bi_connector_chyt_internal.core.constants import (
    SOURCE_TYPE_CHYT_SUBSELECT,
    SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT,
)
from bi_connector_chyt_internal.core.data_source import (
    CHYTInternalTableSubselectDataSource, CHYTUserAuthTableSubselectDataSource
)

from bi_connector_chyt_internal_tests.ext.core.base import BaseCHYTTestClass, BaseCHYTUserAuthTestClass


SAMPLE_TABLE_SCHEMA_SUPERSTORE_CHYTIZED = [
    (name, user_type if user_type != BIType.date else BIType.string)
    for name, user_type in TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema
]


class TestCHYTDataSource(
    BaseCHYTTestClass,
    DefaultDataSourceTestClass[
        ConnectionCHYTInternalToken,
        CHYTSubselectDataSourceSpec,
        CHYTInternalTableSubselectDataSource,
    ]
):
    DSRC_CLS = CHYTInternalTableSubselectDataSource
    source_type = SOURCE_TYPE_CHYT_SUBSELECT

    def get_expected_simplified_schema(self) -> list[tuple[str, BIType]]:
        return SAMPLE_TABLE_SCHEMA_SUPERSTORE_CHYTIZED

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self, sample_table) -> CHYTSubselectDataSourceSpec:
        return CHYTSubselectDataSourceSpec(
            source_type=SOURCE_TYPE_CHYT_SUBSELECT,
            subsql=f'select * from "{sample_table.name}"',
        )


class TestCHYTUserAuthDataSource(
    BaseCHYTUserAuthTestClass,
    DefaultDataSourceTestClass[
        ConnectionCHYTUserAuth,
        CHYTSubselectDataSourceSpec,
        CHYTUserAuthTableSubselectDataSource,
    ]
):
    DSRC_CLS = CHYTUserAuthTableSubselectDataSource
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT

    def get_expected_simplified_schema(self) -> list[tuple[str, BIType]]:
        return SAMPLE_TABLE_SCHEMA_SUPERSTORE_CHYTIZED

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self, sample_table) -> CHYTSubselectDataSourceSpec:
        return CHYTSubselectDataSourceSpec(
            source_type=SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT,
            subsql=f'select * from "{sample_table.name}"',
        )
