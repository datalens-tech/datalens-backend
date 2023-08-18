from __future__ import annotations

import marshmallow as ma

from bi_connector_chyt.core.us_connection import ConnectionCHYTToken

from bi_api_connector.api_schema.connection_base import ConnectionSchema, ConnectionMetaMixin
from bi_api_connector.api_schema.connection_base_fields import cache_ttl_field, secret_string_field
from bi_api_connector.api_schema.connection_mixins import RawSQLLevelMixin, DataExportForbiddenMixin
from bi_api_connector.api_schema.connection_sql import DBHostField
from bi_api_connector.api_schema.extras import FieldExtra


class CHYTConnectionSchema(ConnectionMetaMixin, RawSQLLevelMixin, DataExportForbiddenMixin, ConnectionSchema):
    TARGET_CLS = ConnectionCHYTToken

    ALLOW_MULTIHOST = False

    host = DBHostField(attribute='data.host', required=True, bi_extra=FieldExtra(editable=True))
    port = ma.fields.Integer(attribute='data.port', required=True, bi_extra=FieldExtra(editable=True))
    token = secret_string_field(attribute='data.token', required=True, bi_extra=FieldExtra(editable=True))
    alias = ma.fields.String(attribute='data.alias', required=True, bi_extra=FieldExtra(editable=True))
    secure = ma.fields.Boolean(attribute='data.secure', bi_extra=FieldExtra(editable=True))
    cache_ttl_sec = cache_ttl_field(attribute='data.cache_ttl_sec')
