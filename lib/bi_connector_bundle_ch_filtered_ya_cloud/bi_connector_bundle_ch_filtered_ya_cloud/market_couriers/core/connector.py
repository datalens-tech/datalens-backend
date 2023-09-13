from __future__ import annotations

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreSourceDefinition,
)
from bi_connector_clickhouse.core.clickhouse_base.connector import ClickHouseCoreConnectorBase
from bi_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor,
)
from bi_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from bi_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.constants import (
    CONNECTION_TYPE_MARKET_COURIERS, SOURCE_TYPE_CH_MARKET_COURIERS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.lifecycle import (
    CHFilteredSubselectByPuidBaseConnectionLifecycleManager,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.storage_schemas.connection import (
    ConnectionCHFilteredSubselectByPuidDataStorageSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.data_source import ClickHouseMarketCouriersDataSource
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.settings import CHMarketCouriersSettingDefinition
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.us_connection import (
    ConnectionClickhouseMarketCouriers,
)


class CHMarketCouriersCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_MARKET_COURIERS
    connection_cls = ConnectionClickhouseMarketCouriers
    us_storage_schema_cls = ConnectionCHFilteredSubselectByPuidDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    lifecycle_manager_cls = CHFilteredSubselectByPuidBaseConnectionLifecycleManager
    dialect_string = 'bi_clickhouse'
    settings_definition = CHMarketCouriersSettingDefinition


class CHMarketCouriersCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_MARKET_COURIERS_TABLE
    source_cls = ClickHouseMarketCouriersDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class CHMarketCouriersCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (
        CHMarketCouriersCoreConnectionDefinition,
    )
    source_definitions = (
        CHMarketCouriersCoreSourceDefinition,
    )
