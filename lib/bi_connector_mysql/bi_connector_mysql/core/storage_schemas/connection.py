from bi_connector_mysql.core.us_connection import ConnectionMySQL
from bi_core.us_manager.storage_schemas.connection import (
    ConnectionSQLDataStorageSchema,
    ConnectionMDBStorageDataSchemaMixin,
)


class ConnectionMySQLDataStorageSchema(
        ConnectionMDBStorageDataSchemaMixin,
        ConnectionSQLDataStorageSchema[ConnectionMySQL.DataModel],
):
    TARGET_CLS = ConnectionMySQL.DataModel
