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

from bi_connector_mdb_base.bi.api_schema.connection_mixins import MDBDatabaseSchemaMixin
from bi_connector_yql.core.ydb.us_connection import YDBConnection


class YDBConnectionSchema(RawSQLLevelMixin, MDBDatabaseSchemaMixin, ConnectionSchema):
    TARGET_CLS = YDBConnection

    host = DBHostField(attribute="data.host", required=True, bi_extra=FieldExtra(editable=True))
    port = ma_fields.Integer(attribute="data.port", required=True, bi_extra=FieldExtra(editable=True))
    db_name = ma_fields.String(attribute="data.db_name", required=True, bi_extra=FieldExtra(editable=True))

    # TODO: require either `folder_id` + `service_account_id`, or `token`.
    folder_id = ma_fields.String(
        attribute="data.folder_id",
        required=False,
        allow_none=True,
        bi_extra=FieldExtra(editable=True),
    )
    service_account_id = ma_fields.String(
        attribute="data.service_account_id",
        required=False,
        allow_none=True,
        bi_extra=FieldExtra(editable=True),
    )
    token = secret_string_field(attribute="data.token", required=False, allow_none=True)
    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")
