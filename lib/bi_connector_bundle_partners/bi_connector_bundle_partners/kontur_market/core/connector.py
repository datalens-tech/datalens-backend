from dl_constants.enums import ConnectionType

from dl_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreSourceDefinition,
)
from dl_connector_clickhouse.core.clickhouse_base.connector import ClickHouseCoreConnectorBase
from dl_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor,
)
from dl_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from bi_connector_bundle_partners.base.core.storage_schemas.connection import PartnersCHConnectionDataStorageSchema
from dl_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_partners.kontur_market.core.constants import (
    CONNECTION_TYPE_KONTUR_MARKET,
    SOURCE_TYPE_KONTUR_MARKET_CH_TABLE,
)
from bi_connector_bundle_partners.kontur_market.core.data_source import KonturMarketCHDataSource
from bi_connector_bundle_partners.kontur_market.core.settings import KonturMarketSettingDefinition
from bi_connector_bundle_partners.kontur_market.core.us_connection import KonturMarketCHConnection


class KonturMarketCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_KONTUR_MARKET
    connection_cls = KonturMarketCHConnection
    us_storage_schema_cls = PartnersCHConnectionDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    dialect_string = 'bi_clickhouse'
    settings_definition = KonturMarketSettingDefinition


class KonturMarketTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_KONTUR_MARKET_CH_TABLE
    source_cls = KonturMarketCHDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class KonturMarketCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (
        KonturMarketCoreConnectionDefinition,
    )
    source_definitions = (
        KonturMarketTableCoreSourceDefinition,
    )
