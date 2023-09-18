from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import (
    CacheableConnectionDataSchemaMixin,
    ConnectionBaseDataStorageSchema,
)

from bi_connector_solomon.core.us_connection import SolomonConnection


class ConnectionSolomonDataStorageSchema(
    CacheableConnectionDataSchemaMixin, ConnectionBaseDataStorageSchema[SolomonConnection.DataModel]
):
    TARGET_CLS = SolomonConnection.DataModel

    host = ma_fields.String(required=True, allow_none=False)
