from marshmallow import fields as ma_fields
from marshmallow.validate import Regexp

from dl_core.us_manager.storage_schemas.connection import (
    BaseConnectionDataStorageSchema,
    CacheableConnectionDataSchemaMixin,
    SubselectConnectionDataSchemaMixin,
)

from dl_connector_snowflake.core.constants import ACCOUNT_NAME_RE
from dl_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake


class SnowFlakeConnectionDataStorageSchema(
    BaseConnectionDataStorageSchema[ConnectionSQLSnowFlake.DataModel],
    CacheableConnectionDataSchemaMixin,
    SubselectConnectionDataSchemaMixin,
):
    TARGET_CLS = ConnectionSQLSnowFlake.DataModel

    account_name = ma_fields.String(allow_none=False, required=True, validate=Regexp(regex=ACCOUNT_NAME_RE))
    user_name = ma_fields.String(allow_none=False, required=True)
    user_role = ma_fields.String(allow_none=True, required=False)
    client_id = ma_fields.String(allow_none=False, required=True)
    client_secret = ma_fields.String(allow_none=False, required=True)
    refresh_token = ma_fields.String(allow_none=False, required=True)
    refresh_token_expire_time = ma_fields.DateTime(allow_none=True, required=False)

    schema = ma_fields.String(allow_none=False, required=True)
    db_name = ma_fields.String(allow_none=False, required=True)
    warehouse = ma_fields.String(allow_none=False, required=True)
