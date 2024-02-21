from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema

from dl_connector_ydb.core.ydb.constants import YDBAuthTypeMode
from dl_connector_ydb.core.ydb.us_connection import YDBConnection


class YDBConnectionDataStorageSchema(ConnectionSQLDataStorageSchema[YDBConnection.DataModel]):
    TARGET_CLS = YDBConnection.DataModel

    token = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)
    auth_type = ma_fields.String(
        required=False,
        allow_none=True,
        dump_default=YDBAuthTypeMode.oauth.value,
        load_default=YDBAuthTypeMode.oauth.value,
    )

    username = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)
    password = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)
