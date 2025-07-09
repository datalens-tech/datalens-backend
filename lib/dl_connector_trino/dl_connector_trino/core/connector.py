from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)
from dl_core.connectors.sql_base.connector import SQLSubselectCoreSourceDefinitionBase
from dl_core.data_source_spec.sql import StandardSchemaSQLDataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import SchemaSQLDataSourceSpecStorageSchema

from dl_connector_trino.core.adapters import TrinoDefaultAdapter
from dl_connector_trino.core.connection_executors import TrinoConnExecutor
from dl_connector_trino.core.constants import (
    BACKEND_TYPE_TRINO,
    CONNECTION_TYPE_TRINO,
    SOURCE_TYPE_TRINO_SUBSELECT,
    SOURCE_TYPE_TRINO_TABLE,
)
from dl_connector_trino.core.data_source import (
    TrinoSubselectDataSource,
    TrinoTableDataSource,
)
from dl_connector_trino.core.data_source_migration import TrinoDataSourceMigrator
from dl_connector_trino.core.query_compiler import TrinoQueryCompiler
from dl_connector_trino.core.storage_schemas.connection import (
    TrinoConnectionDataStorageSchema,
    TrinoConnectionDataStorageSchemaBase,
)
from dl_connector_trino.core.type_transformer import TrinoTypeTransformer
from dl_connector_trino.core.us_connection import (
    ConnectionTrino,
    ConnectionTrinoBase,
)


class TrinoCoreConnectionDefinitionBase(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_TRINO
    connection_cls = ConnectionTrinoBase
    us_storage_schema_cls = TrinoConnectionDataStorageSchemaBase
    type_transformer_cls = TrinoTypeTransformer
    data_source_migrator_cls = TrinoDataSourceMigrator
    dialect_string = "trino"
    allow_export = True


class TrinoCoreConnectionDefinition(TrinoCoreConnectionDefinitionBase):
    connection_cls = ConnectionTrino
    us_storage_schema_cls = TrinoConnectionDataStorageSchema
    sync_conn_executor_cls = TrinoConnExecutor


class TrinoCoreTableSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_TRINO_TABLE
    source_cls = TrinoTableDataSource
    source_spec_cls = StandardSchemaSQLDataSourceSpec
    us_storage_schema_cls = SchemaSQLDataSourceSpecStorageSchema


class TrinoCoreSubselectSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_TRINO_SUBSELECT
    source_cls = TrinoSubselectDataSource


class TrinoCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_TRINO
    compiler_cls = TrinoQueryCompiler


class TrinoCoreConnectorBase(CoreConnector):
    backend_definition = TrinoCoreBackendDefinition
    connection_definitions = (TrinoCoreConnectionDefinitionBase,)
    source_definitions = (
        TrinoCoreTableSourceDefinition,
        TrinoCoreSubselectSourceDefinition,
    )
    subselect_source_definition_cls = TrinoCoreSubselectSourceDefinition
    rqe_adapter_classes = frozenset({TrinoDefaultAdapter})


class TrinoCoreConnector(TrinoCoreConnectorBase):
    connection_definitions = (TrinoCoreConnectionDefinition,)
