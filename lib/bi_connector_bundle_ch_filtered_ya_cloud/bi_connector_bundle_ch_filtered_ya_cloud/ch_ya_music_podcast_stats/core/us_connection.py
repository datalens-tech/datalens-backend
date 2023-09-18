from __future__ import annotations

from typing import ClassVar

from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.constants import (
    SOURCE_TYPE_CH_YA_MUSIC_PODCAST_STATS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.settings import (
    CHYaMusicPodcastStatsConnectorSettings,
)


class ConnectionClickhouseYaMusicPodcastStats(
    ConnectionCHFilteredSubselectByPuidBase[CHYaMusicPodcastStatsConnectorSettings]
):
    source_type = SOURCE_TYPE_CH_YA_MUSIC_PODCAST_STATS_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_YA_MUSIC_PODCAST_STATS_TABLE,))
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    settings_type = CHYaMusicPodcastStatsConnectorSettings
