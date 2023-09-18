from marshmallow import fields as ma_fields

from bi_connector_yql.core.yq.us_connection import YQConnection
from dl_core.us_manager.storage_schemas.connection import (
    ConnectionBaseDataStorageSchema,
    CacheableConnectionDataSchemaMixin,
    SubselectConnectionDataSchemaMixin,
)


class YQConnectionDataStorageSchema(
        ConnectionBaseDataStorageSchema[YQConnection.DataModel],
        CacheableConnectionDataSchemaMixin,
        SubselectConnectionDataSchemaMixin,
):
    TARGET_CLS = YQConnection.DataModel

    service_account_id = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)
    folder_id = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)

    password = ma_fields.String(required=True, allow_none=False)
