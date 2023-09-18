import pytest

from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.constants import (
    SOURCE_TYPE_CH_SCHOOLBOOK_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.us_connection import ConnectionClickhouseSchoolbook
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.data_source import ClickHouseSchoolbookDataSource
from dl_core_testing.testcases.data_source import SQLDataSourceTestClass

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.schoolbook.core.base import BaseSchoolbookTestClass

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import EXT_BLACKBOX_USER_UID
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import (
    SR_CONNECTION_SETTINGS_PARAMS, SR_CONNECTION_TABLE_NAME,
)


class TestClickHouseTableDataSource(
        BaseSchoolbookTestClass,
        SQLDataSourceTestClass[
            ConnectionClickhouseSchoolbook,
            StandardSQLDataSourceSpec,
            ClickHouseSchoolbookDataSource,
        ],
):

    DSRC_CLS = ClickHouseSchoolbookDataSource
    QUERY_PATTERN = f'WHERE int_value = {EXT_BLACKBOX_USER_UID}'

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_CH_SCHOOLBOOK_TABLE,
            db_name=SR_CONNECTION_SETTINGS_PARAMS['DB_NAME'],
            table_name=SR_CONNECTION_TABLE_NAME,
        )
        return dsrc_spec
