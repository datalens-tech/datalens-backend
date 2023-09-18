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

from bi_connector_bundle_ch_filtered_ya_cloud.base.core.lifecycle import (
    CHFilteredSubselectByPuidBaseConnectionLifecycleManager,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.storage_schemas.connection import (
    ConnectionCHFilteredSubselectByPuidDataStorageSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.constants import (
    CONNECTION_TYPE_SMB_HEATMAPS,
    SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.data_source import ClickHouseSMBHeatmapsDataSource
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.settings import CHSMBHeatmapsSettingDefinition
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.us_connection import ConnectionClickhouseSMBHeatmaps


class CHSMBHeatmapsCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_SMB_HEATMAPS
    connection_cls = ConnectionClickhouseSMBHeatmaps
    us_storage_schema_cls = ConnectionCHFilteredSubselectByPuidDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    lifecycle_manager_cls = CHFilteredSubselectByPuidBaseConnectionLifecycleManager
    dialect_string = "bi_clickhouse"
    settings_definition = CHSMBHeatmapsSettingDefinition


class CHSMBHeatmapsCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE
    source_cls = ClickHouseSMBHeatmapsDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class CHSMBHeatmapsCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (CHSMBHeatmapsCoreConnectionDefinition,)
    source_definitions = (CHSMBHeatmapsCoreSourceDefinition,)
