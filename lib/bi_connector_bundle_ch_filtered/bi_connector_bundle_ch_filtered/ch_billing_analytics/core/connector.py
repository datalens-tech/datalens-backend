from __future__ import annotations

from dl_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreSourceDefinition,
)

from dl_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor,
    ClickHouseAsyncAdapterConnExecutor,
)
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema

from dl_connector_clickhouse.core.clickhouse_base.connector import ClickHouseCoreConnectorBase
from dl_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer

from bi_connector_bundle_ch_filtered.base.core.storage_schemas.connection import (
    ConnectionCHFilteredHardcodedDataBaseDataStorageSchema,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.constants import (
    CONNECTION_TYPE_CH_BILLING_ANALYTICS,
    SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.data_source import BillingAnalyticsCHDataSource
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.lifecycle import (
    BillingAnalyticsCHConnectionLifecycleManager,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.settings import BillingAnalyticsSettingDefinition
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.us_connection import BillingAnalyticsCHConnection


class CHBillingAnalyticsCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_BILLING_ANALYTICS
    connection_cls = BillingAnalyticsCHConnection
    us_storage_schema_cls = ConnectionCHFilteredHardcodedDataBaseDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    lifecycle_manager_cls = BillingAnalyticsCHConnectionLifecycleManager
    dialect_string = 'bi_clickhouse'
    settings_definition = BillingAnalyticsSettingDefinition


class CHBillingAnalyticsTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE
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
