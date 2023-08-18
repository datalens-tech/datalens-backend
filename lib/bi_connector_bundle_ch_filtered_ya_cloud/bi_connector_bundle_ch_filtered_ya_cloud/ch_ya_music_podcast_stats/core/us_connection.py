from __future__ import annotations

from typing import ClassVar, TYPE_CHECKING

from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.constants import (
    SOURCE_TYPE_CH_YA_MUSIC_PODCAST_STATS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase

if TYPE_CHECKING:
    from bi_configs.connectors_settings import CHYaMusicPodcastStatsConnectorSettings


class ConnectionClickhouseYaMusicPodcastStats(ConnectionCHFilteredSubselectByPuidBase):
    source_type = SOURCE_TYPE_CH_YA_MUSIC_PODCAST_STATS_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_YA_MUSIC_PODCAST_STATS_TABLE,))
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True

    @property
    def _connector_settings(self) -> CHYaMusicPodcastStatsConnectorSettings:
        settings = self._all_connectors_settings.CH_YA_MUSIC_PODCAST_STATS
        assert settings is not None
        return settings
