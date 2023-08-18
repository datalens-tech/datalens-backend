from marshmallow import fields as ma_fields

from bi_connector_yql.core.ydb.us_connection import YDBConnection
from bi_core.us_manager.storage_schemas.connection import (
    ConnectionSQLDataStorageSchema,
    ConnectionMDBStorageDataSchemaMixin,
)


class YDBConnectionDataStorageSchema(
        ConnectionSQLDataStorageSchema[YDBConnection.DataModel],
        ConnectionMDBStorageDataSchemaMixin,
):
    TARGET_CLS = YDBConnection.DataModel

    token = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)
    service_account_id = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)
    folder_id = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)

    username = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)
    password = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)
