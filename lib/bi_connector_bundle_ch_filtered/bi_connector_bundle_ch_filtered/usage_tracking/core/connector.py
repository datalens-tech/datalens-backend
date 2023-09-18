from __future__ import annotations

from dl_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseAsyncAdapterConnExecutor,
    ClickHouseSyncAdapterConnExecutor,
)
from dl_connector_clickhouse.core.clickhouse_base.connector import ClickHouseCoreConnectorBase
from dl_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from dl_core.connectors.base.connector import (
    CoreConnectionDefinition,
    CoreSourceDefinition,
)
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema

from bi_connector_bundle_ch_filtered.base.core.storage_schemas.connection import (
    ConnectionCHFilteredHardcodedDataBaseDataStorageSchema,
)
from bi_connector_bundle_ch_filtered.usage_tracking.core.constants import (
    CONNECTION_TYPE_USAGE_TRACKING,
    SOURCE_TYPE_CH_USAGE_TRACKING_TABLE,
)
from bi_connector_bundle_ch_filtered.usage_tracking.core.data_source import UsageTrackingDataSource
from bi_connector_bundle_ch_filtered.usage_tracking.core.lifecycle import UsageTrackingConnectionLifecycleManager
from bi_connector_bundle_ch_filtered.usage_tracking.core.settings import UsageTrackingSettingDefinition
from bi_connector_bundle_ch_filtered.usage_tracking.core.us_connection import UsageTrackingConnection


class UsageTrackingCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_USAGE_TRACKING
    connection_cls = UsageTrackingConnection
    us_storage_schema_cls = ConnectionCHFilteredHardcodedDataBaseDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    lifecycle_manager_cls = UsageTrackingConnectionLifecycleManager
    dialect_string = "bi_clickhouse"
    settings_definition = UsageTrackingSettingDefinition


class UsageTrackingCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_USAGE_TRACKING_TABLE
    source_cls = UsageTrackingDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class UsageTrackingCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (UsageTrackingCoreConnectionDefinition,)
    source_definitions = (UsageTrackingCoreSourceDefinition,)
