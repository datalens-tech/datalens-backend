from marshmallow import fields as ma_fields

from bi_connector_gsheets.core.us_connection import GSheetsConnection
from bi_core.us_manager.storage_schemas.connection import (
    CacheableConnectionDataSchemaMixin,
    ConnectionBaseDataStorageSchema,
)


class GSheetsConnectionDataStorageSchema(
        CacheableConnectionDataSchemaMixin,
        ConnectionBaseDataStorageSchema[GSheetsConnection.DataModel]
):
    TARGET_CLS = GSheetsConnection.DataModel

    url = ma_fields.String(required=True, allow_none=False)
