from __future__ import annotations

from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import ConnectionSchema
from dl_api_connector.api_schema.connection_base_fields import (
    cache_ttl_field,
    secret_string_field,
)
from dl_api_connector.api_schema.connection_mixins import RawSQLLevelMixin
from dl_api_connector.api_schema.connection_sql import DBHostField
from dl_api_connector.api_schema.extras import FieldExtra

from dl_connector_ydb.core.ydb.constants import YDBAuthTypeMode
from dl_connector_ydb.core.ydb.us_connection import YDBConnection


class YDBConnectionSchema(RawSQLLevelMixin, ConnectionSchema):
    TARGET_CLS = YDBConnection

    auth_type = ma_fields.Enum(
        YDBAuthTypeMode,
        attribute="data.auth_type",
        required=False,
        allow_none=True,
        default=YDBAuthTypeMode.oauth,
        bi_extra=FieldExtra(editable=True),
    )

    host = DBHostField(attribute="data.host", required=True, bi_extra=FieldExtra(editable=True))
    port = ma_fields.Integer(attribute="data.port", required=True, bi_extra=FieldExtra(editable=True))
    db_name = ma_fields.String(attribute="data.db_name", required=True, bi_extra=FieldExtra(editable=True))

    token = secret_string_field(attribute="data.token", required=False, allow_none=True)

    username = ma_fields.String(
        attribute="data.username", required=False, allow_none=True, bi_extra=FieldExtra(editable=True)
    )

    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")
