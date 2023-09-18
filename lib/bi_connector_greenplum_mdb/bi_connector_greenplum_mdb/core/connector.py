from bi_core.connections_security.base import ConnSecuritySettings
from bi_api_lib_ya.connections_security.base import MDBConnectionSafetyChecker
from bi_connector_greenplum.core.connector import (
    GreenplumCoreConnectionDefinition,
    GreenplumCoreConnector,
    GreenplumTableCoreSourceDefinition,
    GreenplumSubselectCoreSourceDefinition,
)
from bi_connector_greenplum.core.dto import GreenplumConnDTO

from bi_connector_greenplum_mdb.core.data_source import GreenplumMDBTableDataSource, GreenplumMDBSubselectDataSource
from bi_connector_greenplum_mdb.core.us_connection import GreenplumMDBConnection
from bi_connector_greenplum_mdb.core.storage_schemas import GreenplumMDBConnectionDataStorageSchema
from bi_connector_greenplum_mdb.core.settings import GreenplumMDBSettingDefinition
from bi_connector_greenplum_mdb.core.connection_executors import GreenplumMDBConnExecutor, AsyncGreenplumMDBConnExecutor


class GreenplumMDBCoreConnectionDefinition(GreenplumCoreConnectionDefinition):
    connection_cls = GreenplumMDBConnection
    us_storage_schema_cls = GreenplumMDBConnectionDataStorageSchema
    settings_definition = GreenplumMDBSettingDefinition
    sync_conn_executor_cls = GreenplumMDBConnExecutor
    async_conn_executor_cls = AsyncGreenplumMDBConnExecutor


class GreenplumMDBTableCoreSourceDefinition(GreenplumTableCoreSourceDefinition):
    source_cls = GreenplumMDBTableDataSource


class GreenplumMDBSubselectCoreSourceDefinition(GreenplumSubselectCoreSourceDefinition):
    source_cls = GreenplumMDBSubselectDataSource


class GreenplumMDBCoreConnector(GreenplumCoreConnector):
    connection_definitions = (
        GreenplumMDBCoreConnectionDefinition,
    )
    source_definitions = (
        GreenplumMDBTableCoreSourceDefinition,
        GreenplumMDBSubselectCoreSourceDefinition,
    )
    conn_security = frozenset({
        ConnSecuritySettings(MDBConnectionSafetyChecker, frozenset({GreenplumConnDTO})),
    })
