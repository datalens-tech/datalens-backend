from __future__ import annotations

from dl_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreSourceDefinition,
)
from dl_connector_clickhouse.core.clickhouse_base.connector import ClickHouseCoreConnectorBase
from dl_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor,
)
from bi_connector_bundle_ch_filtered.base.core.storage_schemas.connection import (
    ConnectionCHFilteredHardcodedDataBaseDataStorageSchema,
)
from dl_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from dl_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_usage_tracking_ya_team.core.constants import (
    CONNECTION_TYPE_USAGE_TRACKING_YA_TEAM,
    SOURCE_TYPE_CH_USAGE_TRACKING_YA_TEAM_TABLE,
)
from bi_connector_usage_tracking_ya_team.core.data_source import UsageTrackingYaTeamDataSource
from bi_connector_usage_tracking_ya_team.core.settings import UsageTrackingYaTeamSettingDefinition
from bi_connector_usage_tracking_ya_team.core.us_connection import UsageTrackingYaTeamConnection


class UsageTrackingYaTeamCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_USAGE_TRACKING_YA_TEAM
    connection_cls = UsageTrackingYaTeamConnection
    us_storage_schema_cls = ConnectionCHFilteredHardcodedDataBaseDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    dialect_string = 'bi_clickhouse'
    settings_definition = UsageTrackingYaTeamSettingDefinition


class UsageTrackingYaTeamCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_USAGE_TRACKING_YA_TEAM_TABLE
    source_cls = UsageTrackingYaTeamDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class UsageTrackingYaTeamCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (
        UsageTrackingYaTeamCoreConnectionDefinition,
    )
    source_definitions = (
        UsageTrackingYaTeamCoreSourceDefinition,
    )
