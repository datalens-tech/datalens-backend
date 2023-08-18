from __future__ import annotations

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreSourceDefinition,
)
from bi_core.connectors.clickhouse_base.connector import ClickHouseCoreConnectorBase
from bi_core.connectors.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor,
)
from bi_core.connectors.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec
from bi_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema

from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.constants import (
    CONNECTION_TYPE_CH_YA_MUSIC_PODCAST_STATS, SOURCE_TYPE_CH_YA_MUSIC_PODCAST_STATS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.lifecycle import (
    CHFilteredSubselectByPuidBaseConnectionLifecycleManager,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.storage_schemas.connection import (
    ConnectionCHFilteredSubselectByPuidDataStorageSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.data_source import (
    ClickHouseYaMusicPodcastStatsDataSource,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.us_connection import (
    ConnectionClickhouseYaMusicPodcastStats,
)


class CHYaMusicPodcastStatsCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_YA_MUSIC_PODCAST_STATS
    connection_cls = ConnectionClickhouseYaMusicPodcastStats
    us_storage_schema_cls = ConnectionCHFilteredSubselectByPuidDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    lifecycle_manager_cls = CHFilteredSubselectByPuidBaseConnectionLifecycleManager
    dialect_string = 'bi_clickhouse'


class CHYaMusicPodcastStatsTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_YA_MUSIC_PODCAST_STATS_TABLE
    source_cls = ClickHouseYaMusicPodcastStatsDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class CHYaMusicPodcastStatsCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (
        CHYaMusicPodcastStatsCoreConnectionDefinition,
    )
    source_definitions = (
        CHYaMusicPodcastStatsTableCoreSourceDefinition,
    )
