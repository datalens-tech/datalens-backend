from typing import Set

from marshmallow import fields as ma_fields

from bi_core.connectors.chydb.us_connection import ConnectionCHYDB

from bi_api_connector.api_schema.extras import FieldExtra
from bi_api_connector.api_schema.connection_base import ConnectionSchema, ConnectionMetaMixin
from bi_api_connector.api_schema.connection_base_fields import cache_ttl_field, secret_string_field
from bi_api_connector.api_schema.connection_mixins import RawSQLLevelMixin
from bi_api_connector.api_schema.connection_sql import DBHostField


class CHYDBConnectionSchema(ConnectionMetaMixin, RawSQLLevelMixin, ConnectionSchema):
    # Side note: previously this schema was inherited from ClassicSQLConnectionSchema
    #  username, password and db_name fields was ignored
    TARGET_CLS = ConnectionCHYDB

    host = DBHostField(attribute='data.host', required=True, bi_extra=FieldExtra(editable=True))
    port = ma_fields.Integer(attribute='data.port', required=True, bi_extra=FieldExtra(editable=True))
    default_ydb_cluster = ma_fields.String(
        attribute='data.default_ydb_cluster',
        required=False, allow_none=True, default=None,
        bi_extra=FieldExtra(editable=True),
    )
    default_ydb_database = ma_fields.String(
        attribute='data.default_ydb_database',
        required=False, allow_none=True, default=None,
        bi_extra=FieldExtra(editable=True),
    )
    secure = ma_fields.Boolean(attribute='data.secure', bi_extra=FieldExtra(editable=True))
    token = secret_string_field(attribute='data.token')
    cache_ttl_sec = cache_ttl_field(attribute='data.cache_ttl_sec')

    def get_allowed_unknown_fields(self) -> Set[str]:
        super_fields = super().get_allowed_unknown_fields()
        super_fields.update(
            'username',
            'password',
            'db_name',
        )
        return super_fields
