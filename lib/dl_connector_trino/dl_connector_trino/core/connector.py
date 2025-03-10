from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)
from dl_core.connectors.sql_base.connector import SQLSubselectCoreSourceDefinitionBase
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema

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
from dl_connector_trino.core.storage_schemas.connection import TrinoConnectionDataStorageSchema
from dl_connector_trino.core.type_transformer import TrinoTypeTransformer
from dl_connector_trino.core.us_connection import ConnectionTrino


# from dl_connector_trino.core.lifecycle import TrinoConnectionLifecycleManager


class TrinoCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_TRINO
    connection_cls = ConnectionTrino
    us_storage_schema_cls = TrinoConnectionDataStorageSchema
    type_transformer_cls = TrinoTypeTransformer
    sync_conn_executor_cls = TrinoConnExecutor
    # lifecycle_manager_cls = TrinoConnectionLifecycleManager
    data_source_migrator_cls = TrinoDataSourceMigrator
    dialect_string = "trino"


class TrinoCoreTableSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_TRINO_TABLE
    source_cls = TrinoTableDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class TrinoCoreSubselectSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_TRINO_SUBSELECT
    source_cls = TrinoSubselectDataSource


class TrinoCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_TRINO
    # compiler_cls = ?


class TrinoCoreConnector(CoreConnector):
    backend_definition = TrinoCoreBackendDefinition
    connection_definitions = (TrinoCoreConnectionDefinition,)
    source_definitions = (
        TrinoCoreTableSourceDefinition,
        TrinoCoreSubselectSourceDefinition,
    )
    subselect_source_definition_cls = TrinoCoreSubselectSourceDefinition
    rqe_adapter_classes = frozenset({TrinoDefaultAdapter})
    # notification_classes = ?
    # sa_types = ?
