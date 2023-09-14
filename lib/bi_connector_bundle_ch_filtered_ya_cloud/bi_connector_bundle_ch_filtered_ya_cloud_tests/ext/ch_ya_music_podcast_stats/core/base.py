from __future__ import annotations

import pytest

from bi_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.constants import (
    CONNECTION_TYPE_CH_YA_MUSIC_PODCAST_STATS,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.settings import (
    CHYaMusicPodcastStatsConnectorSettings,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.us_connection import (
    ConnectionClickhouseYaMusicPodcastStats,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.testing.connection import (
    make_saved_ch_ya_music_podcast_stats_connection,
)

import bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config as test_config
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.base import (
    BaseClickhouseFilteredSubselectByPuidTestClass, ClickhouseFilteredSubselectByPuidTestClassWithWrongAuth,
)


class BaseClickhouseYaMusicPodcastStatsTestClass(
        BaseClickhouseFilteredSubselectByPuidTestClass[ConnectionClickhouseYaMusicPodcastStats]
):
    conn_type = CONNECTION_TYPE_CH_YA_MUSIC_PODCAST_STATS
    connection_settings = CHYaMusicPodcastStatsConnectorSettings(**test_config.SR_CONNECTION_SETTINGS_PARAMS)

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict
    ) -> ConnectionClickhouseYaMusicPodcastStats:
        conn = make_saved_ch_ya_music_podcast_stats_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )
        return sync_us_manager.get_by_id(conn.uuid)  # to invoke a lifecycle manager


class ClickhouseYaMusicPodcastStatsTestClassWithWrongAuth(
        BaseClickhouseYaMusicPodcastStatsTestClass,
        ClickhouseFilteredSubselectByPuidTestClassWithWrongAuth[ConnectionClickhouseYaMusicPodcastStats]
):
    pass
