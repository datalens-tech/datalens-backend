from dl_connector_clickhouse.core.clickhouse.constants import (
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)
from dl_connector_clickhouse.core.clickhouse.data_source import (
    ClickHouseDataSource,
    ClickHouseSubselectDataSource,
)
from dl_connector_clickhouse.core.clickhouse.data_source_migration import ClickHouseDataSourceMigrator
from dl_connector_clickhouse.core.clickhouse.storage_schemas.connection import ConnectionClickhouseDataStorageSchema
from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseAsyncAdapterConnExecutor,
    ClickHouseSyncAdapterConnExecutor,
)
from dl_connector_clickhouse.core.clickhouse_base.connector import ClickHouseCoreConnectorBase
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from dl_core.connectors.base.connector import (
    CoreConnectionDefinition,
    CoreSourceDefinition,
)
from dl_core.connectors.sql_base.connector import SQLSubselectCoreSourceDefinitionBase
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema


class ClickHouseCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CLICKHOUSE
    connection_cls = ConnectionClickhouse
    us_storage_schema_cls = ConnectionClickhouseDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    dialect_string = "bi_clickhouse"
    data_source_migrator_cls = ClickHouseDataSourceMigrator


class ClickHouseTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_TABLE
    source_cls = ClickHouseDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class ClickHouseSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_CH_SUBSELECT
    source_cls = ClickHouseSubselectDataSource


class ClickHouseCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (ClickHouseCoreConnectionDefinition,)
    source_definitions = (
        ClickHouseTableCoreSourceDefinition,
        ClickHouseSubselectCoreSourceDefinition,
    )
