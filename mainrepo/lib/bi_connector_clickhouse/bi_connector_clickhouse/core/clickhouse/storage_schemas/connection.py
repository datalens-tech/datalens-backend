from bi_connector_clickhouse.core.clickhouse_base.storage_schemas.connection import (
    ConnectionClickHouseBaseDataStorageSchema,
)
from bi_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse


class ConnectionClickhouseDataStorageSchema(ConnectionClickHouseBaseDataStorageSchema[ConnectionClickhouse.DataModel]):
    TARGET_CLS = ConnectionClickhouse.DataModel
