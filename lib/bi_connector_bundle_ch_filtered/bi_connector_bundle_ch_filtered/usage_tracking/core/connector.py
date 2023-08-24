from __future__ import annotations

from bi_configs.connectors_settings import UsageTrackingConnectionSettings
from bi_constants.enums import ConnectionType, CreateDSFrom

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreSourceDefinition,
)
from bi_core.connectors.clickhouse_base.connector import ClickHouseCoreConnectorBase
from bi_core.connectors.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor,
)
from bi_core.connectors.clickhouse_base.storage_schemas.connection import (
    ConnectionCHFilteredHardcodedDataBaseDataStorageSchema,
)
from bi_core.connectors.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from bi_connector_bundle_ch_filtered.usage_tracking.core.data_source import UsageTrackingDataSource
from bi_connector_bundle_ch_filtered.usage_tracking.core.lifecycle import UsageTrackingConnectionLifecycleManager
from bi_connector_bundle_ch_filtered.usage_tracking.core.us_connection import UsageTrackingConnection
from bi_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec


class UsageTrackingCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = ConnectionType.usage_tracking
    connection_cls = UsageTrackingConnection
    us_storage_schema_cls = ConnectionCHFilteredHardcodedDataBaseDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    lifecycle_manager_cls = UsageTrackingConnectionLifecycleManager
    dialect_string = 'bi_clickhouse'
    settings_class = UsageTrackingConnectionSettings


class UsageTrackingCoreSourceDefinition(CoreSourceDefinition):
    source_type = CreateDSFrom.CH_USAGE_TRACKING_TABLE
    source_cls = UsageTrackingDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class UsageTrackingCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (
        UsageTrackingCoreConnectionDefinition,
    )
    source_definitions = (
        UsageTrackingCoreSourceDefinition,
    )
