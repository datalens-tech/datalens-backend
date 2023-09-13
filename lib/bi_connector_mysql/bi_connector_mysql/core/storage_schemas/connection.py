from bi_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema
from bi_connector_mdb_base.core.storage_schemas import ConnectionMDBStorageDataSchemaMixin

from bi_connector_mysql.core.us_connection import ConnectionMySQL


class ConnectionMySQLDataStorageSchema(
        ConnectionMDBStorageDataSchemaMixin,
        ConnectionSQLDataStorageSchema[ConnectionMySQL.DataModel],
):
    TARGET_CLS = ConnectionMySQL.DataModel
