from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)
from dl_api_connector.api_schema.connection_base_fields import (
    cache_ttl_field,
    secret_string_field,
)
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.extras import FieldExtra

from bi_connector_chyt_internal.core.us_connection import (
    ConnectionCHYTInternalToken,
    ConnectionCHYTUserAuth,
)


class CHYTConnectionBaseSchema(ConnectionMetaMixin, RawSQLLevelMixin, DataExportForbiddenMixin, ConnectionSchema):
    cluster = ma_fields.String(attribute="data.cluster", required=True, bi_extra=FieldExtra(editable=True))
    alias = ma_fields.String(
        attribute="data.alias",
        required=True,
        bi_extra=FieldExtra(editable=True),
    )
    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")


class CHYTConnectionSchema(CHYTConnectionBaseSchema):
    TARGET_CLS = ConnectionCHYTInternalToken

    token = secret_string_field(attribute="data.token", required=True, bi_extra=FieldExtra(editable=True))


class CHYTUserAuthConnectionSchema(CHYTConnectionBaseSchema):
    TARGET_CLS = ConnectionCHYTUserAuth
