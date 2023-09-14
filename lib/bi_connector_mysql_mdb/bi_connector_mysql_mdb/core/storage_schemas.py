from bi_connector_mdb_base.core.storage_schemas import ConnectionMDBStorageDataSchemaMixin
from bi_connector_mysql.core.storage_schemas.connection import ConnectionMySQLDataStorageSchema
from bi_connector_mysql_mdb.core.us_connection import ConnectionMySQLMDB


class ConnectionMySQLMDBDataStorageSchema(
        ConnectionMDBStorageDataSchemaMixin,
        ConnectionMySQLDataStorageSchema,
):
    TARGET_CLS = ConnectionMySQLMDB.DataModel
