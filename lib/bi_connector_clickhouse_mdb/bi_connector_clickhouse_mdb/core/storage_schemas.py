from dl_connector_clickhouse.core.clickhouse.storage_schemas.connection import ConnectionClickhouseDataStorageSchema
from bi_connector_clickhouse_mdb.core.us_connection import ConnectionClickhouseMDB
from bi_connector_mdb_base.core.storage_schemas import ConnectionMDBStorageDataSchemaMixin


class ConnectionClickhouseMDBDataStorageSchema(
        ConnectionMDBStorageDataSchemaMixin,
        ConnectionClickhouseDataStorageSchema,
):
    TARGET_CLS = ConnectionClickhouseMDB.DataModel
