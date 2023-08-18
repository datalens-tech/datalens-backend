from marshmallow import fields as ma_fields

from bi_connector_chyt_internal.core.us_connection import ConnectionCHYTInternalToken, ConnectionCHYTUserAuth

from bi_api_connector.api_schema.extras import FieldExtra
from bi_api_connector.api_schema.connection_base import ConnectionSchema, ConnectionMetaMixin
from bi_api_connector.api_schema.connection_base_fields import cache_ttl_field, secret_string_field
from bi_api_connector.api_schema.connection_mixins import RawSQLLevelMixin, DataExportForbiddenMixin


class CHYTConnectionBaseSchema(ConnectionMetaMixin, RawSQLLevelMixin, DataExportForbiddenMixin, ConnectionSchema):
    cluster = ma_fields.String(attribute='data.cluster', required=True, bi_extra=FieldExtra(editable=True))
    alias = ma_fields.String(
        attribute='data.alias',
        required=True,
        bi_extra=FieldExtra(editable=True),
    )
    cache_ttl_sec = cache_ttl_field(attribute='data.cache_ttl_sec')

    def get_allowed_unknown_fields(self) -> set[str]:
        super_fields = super().get_allowed_unknown_fields()
        super_fields.update(
            'pool',
            'instance_count',
        )
        return super_fields


class CHYTConnectionSchema(CHYTConnectionBaseSchema):
    TARGET_CLS = ConnectionCHYTInternalToken

    token = secret_string_field(attribute='data.token', required=True, bi_extra=FieldExtra(editable=True))


class CHYTUserAuthConnectionSchema(CHYTConnectionBaseSchema):
    TARGET_CLS = ConnectionCHYTUserAuth
