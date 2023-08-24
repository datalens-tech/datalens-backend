from bi_configs.connectors_settings import ClickHouseConnectorSettings
from bi_constants.enums import ConnectionType, CreateDSFrom

from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec
from bi_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from bi_core.connectors.base.connector import CoreConnectionDefinition, CoreSourceDefinition
from bi_core.connectors.sql_base.connector import SQLSubselectCoreSourceDefinitionBase
from bi_core.connectors.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from bi_core.connectors.clickhouse.us_connection import ConnectionClickhouse
from bi_core.connectors.clickhouse.storage_schemas.connection import ConnectionClickhouseDataStorageSchema
from bi_core.connectors.clickhouse.data_source import ClickHouseDataSource, ClickHouseSubselectDataSource
from bi_core.connectors.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor
)
from bi_core.connectors.clickhouse_base.dto import ClickHouseConnDTO
from bi_core.connectors.clickhouse_base.connector import ClickHouseCoreConnectorBase


class ClickHouseCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = ConnectionType.clickhouse
    connection_cls = ConnectionClickhouse
    us_storage_schema_cls = ConnectionClickhouseDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    dialect_string = 'bi_clickhouse'
    settings_class = ClickHouseConnectorSettings


class ClickHouseTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = CreateDSFrom.CH_TABLE
    source_cls = ClickHouseDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class ClickHouseSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = CreateDSFrom.CH_SUBSELECT
    source_cls = ClickHouseSubselectDataSource


class ClickHouseCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (
        ClickHouseCoreConnectionDefinition,
    )
    source_definitions = (
        ClickHouseTableCoreSourceDefinition,
        ClickHouseSubselectCoreSourceDefinition,
    )
    mdb_dto_classes = frozenset({ClickHouseConnDTO})
