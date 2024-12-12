from __future__ import annotations

import marshmallow as ma

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)
from dl_api_connector.api_schema.connection_base_fields import (
    alias_string_field,
    cache_ttl_field,
    secret_string_field,
)
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.connection_sql import DBHostField
from dl_api_connector.api_schema.extras import FieldExtra

from dl_connector_chyt.core.us_connection import ConnectionCHYTToken


class CHYTConnectionSchema(ConnectionMetaMixin, RawSQLLevelMixin, DataExportForbiddenMixin, ConnectionSchema):
    TARGET_CLS = ConnectionCHYTToken

    ALLOW_MULTIHOST = False

    host = DBHostField(attribute="data.host", required=True, bi_extra=FieldExtra(editable=True))
    port = ma.fields.Integer(attribute="data.port", required=True, bi_extra=FieldExtra(editable=True))
    token = secret_string_field(attribute="data.token", required=True)
    alias = alias_string_field(attribute="data.alias")
    secure = ma.fields.Boolean(attribute="data.secure", bi_extra=FieldExtra(editable=True))
    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")
