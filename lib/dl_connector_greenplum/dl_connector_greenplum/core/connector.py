from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
)
from dl_core.connectors.sql_base.connector import (
    SQLSubselectCoreSourceDefinitionBase,
    SQLTableCoreSourceDefinitionBase,
)

from dl_connector_greenplum.core.adapters import (
    AsyncGreenplumAdapter,
    GreenplumAdapter,
)
from dl_connector_greenplum.core.connection_executors import (
    AsyncGreenplumConnExecutor,
    GreenplumConnExecutor,
)
from dl_connector_greenplum.core.constants import (
    BACKEND_TYPE_GREENPLUM,
    CONNECTION_TYPE_GREENPLUM,
    SOURCE_TYPE_GP_SUBSELECT,
    SOURCE_TYPE_GP_TABLE,
)
from dl_connector_greenplum.core.data_source import (
    GreenplumSubselectDataSource,
    GreenplumTableDataSource,
)
from dl_connector_greenplum.core.data_source_migration import GreenPlumDataSourceMigrator
from dl_connector_greenplum.core.storage_schemas.connection import GreenplumConnectionDataStorageSchema
from dl_connector_greenplum.core.us_connection import GreenplumConnection
from dl_connector_postgresql.core.postgresql_base.query_compiler import PostgreSQLQueryCompiler
from dl_connector_postgresql.core.postgresql_base.sa_types import SQLALCHEMY_POSTGRES_TYPES
from dl_connector_postgresql.core.postgresql_base.type_transformer import PostgreSQLTypeTransformer


class GreenplumCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_GREENPLUM
    connection_cls = GreenplumConnection
    us_storage_schema_cls = GreenplumConnectionDataStorageSchema
    type_transformer_cls = PostgreSQLTypeTransformer
    sync_conn_executor_cls = GreenplumConnExecutor
    async_conn_executor_cls = AsyncGreenplumConnExecutor
    dialect_string = "bi_postgresql"
    data_source_migrator_cls = GreenPlumDataSourceMigrator


class GreenplumTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_GP_TABLE
    source_cls = GreenplumTableDataSource


class GreenplumSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_GP_SUBSELECT
    source_cls = GreenplumSubselectDataSource


class GreenplumCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_GREENPLUM
    compiler_cls = PostgreSQLQueryCompiler


class GreenplumCoreConnector(CoreConnector):
    backend_definition = GreenplumCoreBackendDefinition
    connection_definitions = (GreenplumCoreConnectionDefinition,)
    source_definitions = (
        GreenplumTableCoreSourceDefinition,
        GreenplumSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({GreenplumAdapter, AsyncGreenplumAdapter})
    sa_types = SQLALCHEMY_POSTGRES_TYPES
