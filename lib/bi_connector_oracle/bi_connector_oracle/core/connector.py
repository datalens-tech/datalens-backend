from bi_api_lib_ya.connections_security.base import MDBConnectionSafetyChecker
from bi_core.connections_security.base import ConnSecuritySettings
from bi_core.connectors.base.connector import CoreConnectionDefinition, CoreConnector
from bi_core.connectors.sql_base.connector import (
    SQLTableCoreSourceDefinitionBase,
    SQLSubselectCoreSourceDefinitionBase,
)

from bi_connector_oracle.core.constants import (
    BACKEND_TYPE_ORACLE, CONNECTION_TYPE_ORACLE,
    SOURCE_TYPE_ORACLE_TABLE, SOURCE_TYPE_ORACLE_SUBSELECT,
)
from bi_connector_oracle.core.adapters_oracle import OracleDefaultAdapter
from bi_connector_oracle.core.type_transformer import OracleServerTypeTransformer
from bi_connector_oracle.core.us_connection import ConnectionSQLOracle
from bi_connector_oracle.core.storage_schemas.connection import ConnectionSQLOracleDataStorageSchema
from bi_connector_oracle.core.data_source import OracleDataSource, OracleSubselectDataSource
from bi_connector_oracle.core.connection_executors import OracleDefaultConnExecutor
from bi_connector_oracle.core.dto import OracleConnDTO
from bi_connector_oracle.core.sa_types import SQLALCHEMY_ORACLE_TYPES
from bi_connector_oracle.core.data_source_migration import OracleDataSourceMigrator


class OracleCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_ORACLE
    connection_cls = ConnectionSQLOracle
    us_storage_schema_cls = ConnectionSQLOracleDataStorageSchema
    type_transformer_cls = OracleServerTypeTransformer
    sync_conn_executor_cls = OracleDefaultConnExecutor
    async_conn_executor_cls = OracleDefaultConnExecutor
    dialect_string = 'bi_oracle'
    data_source_migrator_cls = OracleDataSourceMigrator


class OracleTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_ORACLE_TABLE
    source_cls = OracleDataSource


class OracleSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_ORACLE_SUBSELECT
    source_cls = OracleSubselectDataSource


class OracleCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_ORACLE
    connection_definitions = (
        OracleCoreConnectionDefinition,
    )
    source_definitions = (
        OracleTableCoreSourceDefinition,
        OracleSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({OracleDefaultAdapter})
    conn_security = frozenset({
        ConnSecuritySettings(MDBConnectionSafetyChecker, frozenset({OracleConnDTO})),
    })
    sa_types = SQLALCHEMY_ORACLE_TYPES
