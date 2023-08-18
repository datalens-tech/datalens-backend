import pytest

from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.constants import (
    SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.us_connection import (
    ConnectionClickhouseSMBHeatmaps,
)
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.data_source import (
    ClickHouseSMBHeatmapsDataSource
)
from bi_core_testing.testcases.data_source import SQLDataSourceTestClass

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.smb_heatmaps.core.base import BaseSMBHeatmapsTestClass

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import EXT_BLACKBOX_USER_UID
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import (
    SR_CONNECTION_SETTINGS_PARAMS, SR_CONNECTION_TABLE_NAME,
)


class TestClickHouseTableDataSource(
        BaseSMBHeatmapsTestClass,
        SQLDataSourceTestClass[
            ConnectionClickhouseSMBHeatmaps,
            StandardSQLDataSourceSpec,
            ClickHouseSMBHeatmapsDataSource,
        ],
):

    DSRC_CLS = ClickHouseSMBHeatmapsDataSource
    QUERY_PATTERN = f'WHERE int_value = {EXT_BLACKBOX_USER_UID}'

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE,
            db_name=SR_CONNECTION_SETTINGS_PARAMS['DB_NAME'],
            table_name=SR_CONNECTION_TABLE_NAME,
        )
        return dsrc_spec
