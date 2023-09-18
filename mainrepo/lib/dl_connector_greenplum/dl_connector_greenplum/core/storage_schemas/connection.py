from dl_connector_greenplum.core.us_connection import GreenplumConnection
from dl_connector_postgresql.core.postgresql_base.storage_schemas.connection import (
    ConnectionPostgreSQLBaseDataStorageSchema,
)


class GreenplumConnectionDataStorageSchema(ConnectionPostgreSQLBaseDataStorageSchema):
    TARGET_CLS = GreenplumConnection.DataModel
