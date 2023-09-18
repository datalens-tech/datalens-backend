from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema

from bi_connector_mysql.core.us_connection import ConnectionMySQL


class ConnectionMySQLDataStorageSchema(ConnectionSQLDataStorageSchema[ConnectionMySQL.DataModel]):
    TARGET_CLS = ConnectionMySQL.DataModel
