from dl_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseAsyncAdapterConnExecutor,
    ClickHouseSyncAdapterConnExecutor,
)
from dl_connector_clickhouse.core.clickhouse_base.connector import ClickHouseCoreConnectorBase
from dl_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from dl_constants.enums import ConnectionType
from dl_core.connectors.base.connector import (
    CoreConnectionDefinition,
    CoreSourceDefinition,
)
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema

from bi_connector_bundle_partners.base.core.storage_schemas.connection import PartnersCHConnectionDataStorageSchema
from bi_connector_bundle_partners.moysklad.core.constants import (
    CONNECTION_TYPE_MOYSKLAD,
    SOURCE_TYPE_MOYSKLAD_CH_TABLE,
)
from bi_connector_bundle_partners.moysklad.core.data_source import MoySkladCHDataSource
from bi_connector_bundle_partners.moysklad.core.settings import MoySkladSettingDefinition
from bi_connector_bundle_partners.moysklad.core.us_connection import MoySkladCHConnection


class MoySkladCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_MOYSKLAD
    connection_cls = MoySkladCHConnection
    us_storage_schema_cls = PartnersCHConnectionDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    dialect_string = "bi_clickhouse"
    settings_definition = MoySkladSettingDefinition


class MoySkladTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_MOYSKLAD_CH_TABLE
    source_cls = MoySkladCHDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class MoySkladCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (MoySkladCoreConnectionDefinition,)
    source_definitions = (MoySkladTableCoreSourceDefinition,)
