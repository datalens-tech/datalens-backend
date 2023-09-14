from bi_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from bi_connector_postgresql.core.postgresql_base.storage_schemas.connection import (
    ConnectionPostgreSQLBaseDataStorageSchema,
)


class ConnectionPostgreSQLDataStorageSchema(ConnectionPostgreSQLBaseDataStorageSchema):
    TARGET_CLS = ConnectionPostgreSQL.DataModel
