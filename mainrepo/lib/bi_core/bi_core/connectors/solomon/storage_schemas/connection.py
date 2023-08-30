from marshmallow import fields as ma_fields

from bi_core.connectors.solomon.us_connection import SolomonConnection
from bi_core.us_manager.storage_schemas.connection import (
    CacheableConnectionDataSchemaMixin,
    ConnectionBaseDataStorageSchema,
)


class ConnectionSolomonDataStorageSchema(
        CacheableConnectionDataSchemaMixin,
        ConnectionBaseDataStorageSchema[SolomonConnection.DataModel]
):
    TARGET_CLS = SolomonConnection.DataModel

    host = ma_fields.String(required=True, allow_none=False)
