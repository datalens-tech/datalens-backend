from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
)
from dl_core.connectors.sql_base.connector import (
    SQLSubselectCoreSourceDefinitionBase,
    SQLTableCoreSourceDefinitionBase,
)

from dl_connector_oracle.core.adapters_oracle import OracleDefaultAdapter
from dl_connector_oracle.core.connection_executors import OracleDefaultConnExecutor
from dl_connector_oracle.core.constants import (
    BACKEND_TYPE_ORACLE,
    CONNECTION_TYPE_ORACLE,
    SOURCE_TYPE_ORACLE_SUBSELECT,
    SOURCE_TYPE_ORACLE_TABLE,
)
from dl_connector_oracle.core.dashsql import OracleDashSQLParamLiteralizer
from dl_connector_oracle.core.data_source import (
    OracleDataSource,
    OracleSubselectDataSource,
)
from dl_connector_oracle.core.data_source_migration import OracleDataSourceMigrator
from dl_connector_oracle.core.query_compiler import OracleQueryCompiler
from dl_connector_oracle.core.sa_types import SQLALCHEMY_ORACLE_TYPES
from dl_connector_oracle.core.storage_schemas.connection import ConnectionSQLOracleDataStorageSchema
from dl_connector_oracle.core.type_transformer import OracleServerTypeTransformer
from dl_connector_oracle.core.us_connection import ConnectionSQLOracle


class OracleCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_ORACLE
    connection_cls = ConnectionSQLOracle
    us_storage_schema_cls = ConnectionSQLOracleDataStorageSchema
    type_transformer_cls = OracleServerTypeTransformer
    sync_conn_executor_cls = OracleDefaultConnExecutor
    async_conn_executor_cls = OracleDefaultConnExecutor
    dialect_string = "bi_oracle"
    data_source_migrator_cls = OracleDataSourceMigrator


class OracleTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_ORACLE_TABLE
    source_cls = OracleDataSource


class OracleSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_ORACLE_SUBSELECT
    source_cls = OracleSubselectDataSource


class OracleCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_ORACLE
    compiler_cls = OracleQueryCompiler
    dashsql_literalizer_cls = OracleDashSQLParamLiteralizer


class OracleCoreConnector(CoreConnector):
    backend_definition = OracleCoreBackendDefinition
    connection_definitions = (OracleCoreConnectionDefinition,)
    source_definitions = (
        OracleTableCoreSourceDefinition,
        OracleSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({OracleDefaultAdapter})
    sa_types = SQLALCHEMY_ORACLE_TYPES  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "dict[GenericNativeType, function]", base class "CoreConnector" defined the type as "dict[GenericNativeType, Callable[[GenericNativeType], TypeEngine]] | None")  [assignment]
