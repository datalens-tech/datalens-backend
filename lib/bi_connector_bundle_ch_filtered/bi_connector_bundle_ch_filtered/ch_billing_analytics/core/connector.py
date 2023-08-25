from __future__ import annotations

from bi_constants.enums import ConnectionType, CreateDSFrom

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreSourceDefinition,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.data_source import BillingAnalyticsCHDataSource
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.lifecycle import (
    BillingAnalyticsCHConnectionLifecycleManager,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.settings import BillingAnalyticsSettingDefinition
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.us_connection import BillingAnalyticsCHConnection
from bi_core.connectors.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor,
)
from bi_core.connectors.clickhouse_base.storage_schemas.connection import (
    ConnectionCHFilteredHardcodedDataBaseDataStorageSchema,
)
from bi_core.connectors.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from bi_core.connectors.clickhouse_base.connector import ClickHouseCoreConnectorBase
from bi_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec


class CHBillingAnalyticsCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = ConnectionType.ch_billing_analytics
    connection_cls = BillingAnalyticsCHConnection
    us_storage_schema_cls = ConnectionCHFilteredHardcodedDataBaseDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    lifecycle_manager_cls = BillingAnalyticsCHConnectionLifecycleManager
    dialect_string = 'bi_clickhouse'
    settings_definition = BillingAnalyticsSettingDefinition


class CHBillingAnalyticsTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = CreateDSFrom.CH_BILLING_ANALYTICS_TABLE
    source_cls = BillingAnalyticsCHDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class CHBillingAnalyticsCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (
        CHBillingAnalyticsCoreConnectionDefinition,
    )
    source_definitions = (
        CHBillingAnalyticsTableCoreSourceDefinition,
    )
