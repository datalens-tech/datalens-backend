from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema

from dl_connector_mssql.core.us_connection import ConnectionMSSQL


class ConnectionMSSQLDataStorageSchema(ConnectionSQLDataStorageSchema[ConnectionMSSQL.DataModel]):
    TARGET_CLS = ConnectionMSSQL.DataModel
