import pyodbc

from bi_core.connectors.base.connector import CoreConnectionDefinition, CoreConnector
from bi_core.connectors.sql_base.connector import (
    SQLTableCoreSourceDefinitionBase,
    SQLSubselectCoreSourceDefinitionBase,
)

from bi_connector_mssql.core.constants import (
    BACKEND_TYPE_MSSQL, CONNECTION_TYPE_MSSQL,
    SOURCE_TYPE_MSSQL_TABLE, SOURCE_TYPE_MSSQL_SUBSELECT,
)
from bi_connector_mssql.core.storage_schemas.connection import ConnectionMSSQLDataStorageSchema
from bi_connector_mssql.core.adapters_mssql import MSSQLDefaultAdapter
from bi_connector_mssql.core.type_transformer import MSSQLServerTypeTransformer
from bi_connector_mssql.core.us_connection import ConnectionMSSQL
from bi_connector_mssql.core.data_source import MSSQLDataSource, MSSQLSubselectDataSource
from bi_connector_mssql.core.connection_executors import MSSQLConnExecutor
from bi_connector_mssql.core.dto import MSSQLConnDTO
from bi_connector_mssql.core.sa_types import SQLALCHEMY_MSSQL_TYPES


class MSSQLCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_MSSQL
    connection_cls = ConnectionMSSQL
    us_storage_schema_cls = ConnectionMSSQLDataStorageSchema
    type_transformer_cls = MSSQLServerTypeTransformer
    sync_conn_executor_cls = MSSQLConnExecutor
    async_conn_executor_cls = MSSQLConnExecutor
    dialect_string = 'bi_mssql'


class MSSQLTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_MSSQL_TABLE
    source_cls = MSSQLDataSource


class MSSQLSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_MSSQL_SUBSELECT
    source_cls = MSSQLSubselectDataSource


class MSSQLCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_MSSQL
    connection_definitions = (
        MSSQLCoreConnectionDefinition,
    )
    source_definitions = (
        MSSQLTableCoreSourceDefinition,
        MSSQLSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({MSSQLDefaultAdapter})
    mdb_dto_classes = frozenset({MSSQLConnDTO})
    sa_types = SQLALCHEMY_MSSQL_TYPES
    query_fail_exceptions = frozenset({pyodbc.Error})
