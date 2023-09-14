from ydb.sqlalchemy import register_dialect as yql_register_dialect

from bi_api_lib_ya.connections_security.base import MDBConnectionSafetyChecker
from bi_core.connections_security.base import ConnSecuritySettings
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec, SubselectDataSourceSpec
from bi_core.us_manager.storage_schemas.data_source_spec_base import (
    SubselectDataSourceSpecStorageSchema, SQLDataSourceSpecStorageSchema,
)
from bi_core.connectors.base.connector import (
    CoreConnector, CoreConnectionDefinition, CoreSourceDefinition,
)

from bi_connector_yql.core.ydb.constants import (
    BACKEND_TYPE_YDB, CONNECTION_TYPE_YDB, SOURCE_TYPE_YDB_TABLE, SOURCE_TYPE_YDB_SUBSELECT
)
from bi_connector_yql.core.ydb.us_connection import YDBConnection
from bi_connector_yql.core.ydb.storage_schemas.connection import YDBConnectionDataStorageSchema
from bi_connector_yql.core.ydb.type_transformer import YDBTypeTransformer
from bi_connector_yql.core.ydb.connection_executors import YDBAsyncAdapterConnExecutor
from bi_connector_yql.core.ydb.data_source import YDBTableDataSource, YDBSubselectDataSource
from bi_connector_yql.core.ydb.adapter import YDBAdapter
from bi_connector_yql.core.ydb.dto import YDBConnDTO
from bi_connector_yql.core.ydb.settings import YDBSettingDefinition


class YDBCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_YDB
    connection_cls = YDBConnection
    us_storage_schema_cls = YDBConnectionDataStorageSchema
    type_transformer_cls = YDBTypeTransformer
    sync_conn_executor_cls = YDBAsyncAdapterConnExecutor
    async_conn_executor_cls = YDBAsyncAdapterConnExecutor
    dialect_string = 'yql'
    settings_definition = YDBSettingDefinition


class YDBCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_YDB_TABLE
    source_cls = YDBTableDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class YDBCoreSubselectSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_YDB_SUBSELECT
    source_cls = YDBSubselectDataSource
    source_spec_cls = SubselectDataSourceSpec
    us_storage_schema_cls = SubselectDataSourceSpecStorageSchema


class YDBCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_YDB
    connection_definitions = (
        YDBCoreConnectionDefinition,
    )
    source_definitions = (
        YDBCoreSourceDefinition,
        YDBCoreSubselectSourceDefinition,
    )
    rqe_adapter_classes = frozenset({YDBAdapter})
    conn_security = frozenset({
        ConnSecuritySettings(MDBConnectionSafetyChecker, frozenset({YDBConnDTO})),
    })

    @classmethod
    def registration_hook(cls) -> None:
        yql_register_dialect()
