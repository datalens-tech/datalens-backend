from __future__ import annotations

import uuid
from typing import Any

from dl_constants.enums import RawSQLLevel

from dl_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.constants import (
    CONNECTION_TYPE_CH_YA_MUSIC_PODCAST_STATS,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.us_connection import (
    ConnectionClickhouseYaMusicPodcastStats,
)
from dl_core_testing.connection import make_conn_key


def make_saved_ch_ya_music_podcast_stats_connection(
    sync_usm: SyncUSManager,
    endpoint: str,
    cluster_name: str,
    max_execution_time: int,
    token: str,
    secure: bool = False,
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    **kwargs: Any,
) -> ConnectionClickhouseYaMusicPodcastStats:
    conn_name = 'ch_ya_music_podcast_stats test conn {}'.format(uuid.uuid4())
    conn = ConnectionClickhouseYaMusicPodcastStats.create_from_dict(
        ConnectionClickhouseYaMusicPodcastStats.DataModel(
            endpoint=endpoint,
            cluster_name=cluster_name,
            max_execution_time=max_execution_time,
            secure=secure,
            token=token,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=make_conn_key('connections', conn_name),
        type_=CONNECTION_TYPE_CH_YA_MUSIC_PODCAST_STATS.name,
        meta={'title': conn_name, 'state': 'saved'},
        us_manager=sync_usm,
        **kwargs
    )
    sync_usm.save(conn)
    return conn
