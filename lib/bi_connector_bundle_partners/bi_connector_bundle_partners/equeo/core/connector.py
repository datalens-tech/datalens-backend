from bi_constants.enums import ConnectionType

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreSourceDefinition,
)
from bi_core.connectors.clickhouse_base.connector import ClickHouseCoreConnectorBase
from bi_core.connectors.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor,
)
from bi_core.connectors.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from bi_connector_bundle_partners.base.core.storage_schemas.connection import PartnersCHConnectionDataStorageSchema
from bi_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_partners.equeo.core.constants import SOURCE_TYPE_EQUEO_CH_TABLE
from bi_connector_bundle_partners.equeo.core.data_source import EqueoCHDataSource
from bi_connector_bundle_partners.equeo.core.settings import EqueoSettingDefinition
from bi_connector_bundle_partners.equeo.core.us_connection import EqueoCHConnection


class EqueoCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = ConnectionType.equeo
    connection_cls = EqueoCHConnection
    us_storage_schema_cls = PartnersCHConnectionDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    dialect_string = 'bi_clickhouse'
    settings_definition = EqueoSettingDefinition


class EqueoTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_EQUEO_CH_TABLE
    source_cls = EqueoCHDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class EqueoCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (
        EqueoCoreConnectionDefinition,
    )
    source_definitions = (
        EqueoTableCoreSourceDefinition,
    )
