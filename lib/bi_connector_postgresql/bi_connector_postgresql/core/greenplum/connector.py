from bi_core.connectors.base.connector import CoreConnectionDefinition, CoreConnector
from bi_core.connectors.sql_base.connector import (
    SQLTableCoreSourceDefinitionBase,
    SQLSubselectCoreSourceDefinitionBase,
)
from bi_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from bi_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter
from bi_connector_postgresql.core.postgresql_base.type_transformer import PostgreSQLTypeTransformer
from bi_connector_postgresql.core.greenplum.constants import (
    BACKEND_TYPE_GREENPLUM, CONNECTION_TYPE_GREENPLUM,
    SOURCE_TYPE_GP_TABLE, SOURCE_TYPE_GP_SUBSELECT,
)
from bi_connector_postgresql.core.greenplum.us_connection import GreenplumConnection
from bi_connector_postgresql.core.greenplum.storage_schemas.connection import GreenplumConnectionDataStorageSchema
from bi_connector_postgresql.core.greenplum.data_source import GreenplumTableDataSource, GreenplumSubselectDataSource
from bi_connector_postgresql.core.postgresql_base.connection_executors import PostgresConnExecutor, AsyncPostgresConnExecutor
from bi_connector_postgresql.core.greenplum.dto import GreenplumConnDTO
from bi_connector_postgresql.core.postgresql_base.sa_types import SQLALCHEMY_POSTGRES_TYPES
from bi_connector_postgresql.core.greenplum.settings import GreenplumSettingDefinition


class GreenplumCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_GREENPLUM
    connection_cls = GreenplumConnection
    us_storage_schema_cls = GreenplumConnectionDataStorageSchema
    type_transformer_cls = PostgreSQLTypeTransformer
    sync_conn_executor_cls = PostgresConnExecutor
    async_conn_executor_cls = AsyncPostgresConnExecutor
    dialect_string = 'bi_postgresql'
    settings_definition = GreenplumSettingDefinition


class GreenplumTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_GP_TABLE
    source_cls = GreenplumTableDataSource


class GreenplumSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_GP_SUBSELECT
    source_cls = GreenplumSubselectDataSource


class GreenplumCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_GREENPLUM
    connection_definitions = (
        GreenplumCoreConnectionDefinition,
    )
    source_definitions = (
        GreenplumTableCoreSourceDefinition,
        GreenplumSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({PostgresAdapter, AsyncPostgresAdapter})
    mdb_dto_classes = frozenset({GreenplumConnDTO})
    sa_types = SQLALCHEMY_POSTGRES_TYPES
