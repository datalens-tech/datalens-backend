from __future__ import annotations

from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.settings import (
    CHYaMusicPodcastStatsConnectorSettings,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.us_connection import (
    ConnectionClickhouseYaMusicPodcastStats,
)
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.ch_ya_music_podcast_stats.core.base import (
    BaseClickhouseYaMusicPodcastStatsTestClass,
    ClickhouseYaMusicPodcastStatsTestClassWithWrongAuth,
)
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import SR_CONNECTION_SETTINGS_PARAMS
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.connection import (
    BaseClickhouseFilteredSubselectByPuidConnectionTestClass,
    ClickhouseFilteredSubselectByPuidConnectionTestWithWrongAuth,
)


class TestClickhouseYaMusicPodcastStatsConnection(
    BaseClickhouseYaMusicPodcastStatsTestClass,
    BaseClickhouseFilteredSubselectByPuidConnectionTestClass[ConnectionClickhouseYaMusicPodcastStats],
):
    sr_connection_settings = CHYaMusicPodcastStatsConnectorSettings(**SR_CONNECTION_SETTINGS_PARAMS)


class TestClickhouseYaMusicPodcastStatsConnectionWithWrongAuth(
    ClickhouseYaMusicPodcastStatsTestClassWithWrongAuth, ClickhouseFilteredSubselectByPuidConnectionTestWithWrongAuth
):
    pass
