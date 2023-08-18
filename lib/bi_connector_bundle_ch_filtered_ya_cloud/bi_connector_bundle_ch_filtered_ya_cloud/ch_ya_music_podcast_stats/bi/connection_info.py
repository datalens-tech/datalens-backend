from __future__ import annotations

from bi_api_connector.connection_info import ConnectionInfoProvider
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import Translatable


class CHYaMusicPodcastStatsConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-ch_ya_music_podcast_stats')
