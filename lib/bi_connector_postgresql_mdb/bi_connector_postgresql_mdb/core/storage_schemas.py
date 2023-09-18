from dl_connector_postgresql.core.postgresql.storage_schemas.connection import ConnectionPostgreSQLDataStorageSchema

from bi_connector_mdb_base.core.storage_schemas import ConnectionMDBStorageDataSchemaMixin
from bi_connector_postgresql_mdb.core.us_connection import ConnectionPostgreSQLMDB


class ConnectionPostgreSQLMDBDataStorageSchema(
    ConnectionMDBStorageDataSchemaMixin,
    ConnectionPostgreSQLDataStorageSchema,
):
    TARGET_CLS = ConnectionPostgreSQLMDB.DataModel
