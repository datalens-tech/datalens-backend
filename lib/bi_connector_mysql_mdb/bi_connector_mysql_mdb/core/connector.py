from bi_core.connections_security.base import ConnSecuritySettings
from bi_api_lib_ya.connections_security.base import MDBConnectionSafetyChecker
from bi_connector_mysql.core.connector import (
    MySQLCoreConnectionDefinition,
    MySQLTableCoreSourceDefinition,
    MySQLSubselectCoreSourceDefinition,
    MySQLCoreConnector,
)
from bi_connector_mysql.core.dto import MySQLConnDTO
from bi_connector_mysql_mdb.core.data_source import MySQLMDBDataSource, MySQLMDBSubselectDataSource
from bi_connector_mysql_mdb.core.settings import MySQLMDBSettingDefinition
from bi_connector_mysql_mdb.core.storage_schemas import ConnectionMySQLMDBDataStorageSchema
from bi_connector_mysql_mdb.core.us_connection import ConnectionMySQLMDB


class MySQLMDBCoreConnectionDefinition(MySQLCoreConnectionDefinition):
    connection_cls = ConnectionMySQLMDB
    us_storage_schema_cls = ConnectionMySQLMDBDataStorageSchema
    settings_definition = MySQLMDBSettingDefinition


class MySQLMDBTableCoreSourceDefinition(MySQLTableCoreSourceDefinition):
    source_cls = MySQLMDBDataSource


class MySQLMDBSubselectCoreSourceDefinition(MySQLSubselectCoreSourceDefinition):
    source_cls = MySQLMDBSubselectDataSource


class MySQLMDBCoreConnector(MySQLCoreConnector):
    connection_definitions = (
        MySQLMDBCoreConnectionDefinition,
    )
    source_definitions = (
        MySQLMDBTableCoreSourceDefinition,
        MySQLMDBSubselectCoreSourceDefinition,
    )
    conn_security = frozenset({
        ConnSecuritySettings(MDBConnectionSafetyChecker, frozenset({MySQLConnDTO})),
    })
