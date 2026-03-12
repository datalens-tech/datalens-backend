from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema

from dl_connector_starrocks.core.us_connection import ConnectionStarRocks


class ConnectionStarRocksDataStorageSchema(ConnectionSQLDataStorageSchema[ConnectionStarRocks.DataModel]):
    TARGET_CLS = ConnectionStarRocks.DataModel
