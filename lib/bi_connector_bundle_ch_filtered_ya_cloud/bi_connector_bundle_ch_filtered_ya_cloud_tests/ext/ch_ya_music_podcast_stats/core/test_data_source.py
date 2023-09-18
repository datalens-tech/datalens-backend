import pytest

from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.constants import (
    SOURCE_TYPE_CH_YA_MUSIC_PODCAST_STATS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.us_connection import (
    ConnectionClickhouseYaMusicPodcastStats,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.data_source import (
    ClickHouseYaMusicPodcastStatsDataSource
)
from dl_core_testing.testcases.data_source import SQLDataSourceTestClass

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.ch_ya_music_podcast_stats.core.base import (
    BaseClickhouseYaMusicPodcastStatsTestClass
)

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import EXT_BLACKBOX_USER_UID
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import (
    SR_CONNECTION_SETTINGS_PARAMS, SR_CONNECTION_TABLE_NAME,
)


class TestClickHouseTableDataSource(
        BaseClickhouseYaMusicPodcastStatsTestClass,
        SQLDataSourceTestClass[
            ConnectionClickhouseYaMusicPodcastStats,
            StandardSQLDataSourceSpec,
            ClickHouseYaMusicPodcastStatsDataSource,
        ],
):

    DSRC_CLS = ClickHouseYaMusicPodcastStatsDataSource
    QUERY_PATTERN = f'WHERE int_value = {EXT_BLACKBOX_USER_UID}'

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_CH_YA_MUSIC_PODCAST_STATS_TABLE,
            db_name=SR_CONNECTION_SETTINGS_PARAMS['DB_NAME'],
            table_name=SR_CONNECTION_TABLE_NAME,
        )
        return dsrc_spec
