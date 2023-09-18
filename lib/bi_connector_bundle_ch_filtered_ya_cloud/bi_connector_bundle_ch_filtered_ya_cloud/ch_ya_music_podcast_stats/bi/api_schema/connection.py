from __future__ import annotations

from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.us_connection import (
    ConnectionClickhouseYaMusicPodcastStats,
)

from dl_api_connector.api_schema.connection_base import ConnectionSchema, ConnectionMetaMixin
from dl_api_connector.api_schema.connection_base_fields import secret_string_field


class CHYaMusicPodcastStatsConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = ConnectionClickhouseYaMusicPodcastStats

    token = secret_string_field(attribute='data.token')
