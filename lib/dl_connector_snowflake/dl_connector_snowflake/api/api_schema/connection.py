from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import ConnectionSchema
from dl_api_connector.api_schema.connection_base_fields import secret_string_field
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.connection_sql import db_name_no_query_params
from dl_api_connector.api_schema.extras import FieldExtra

from dl_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake


class SnowFlakeConnectionSchema(ConnectionSchema, RawSQLLevelMixin, DataExportForbiddenMixin):
    TARGET_CLS = ConnectionSQLSnowFlake

    account_name = ma_fields.String(
        attribute="data.account_name",
        required=True,
        bi_extra=FieldExtra(editable=True),
    )
    user_name = ma_fields.String(
        attribute="data.user_name",
        required=True,
        bi_extra=FieldExtra(editable=True),
    )
    user_role = ma_fields.String(
        attribute="data.user_role",
        required=False,
        allow_none=True,
        bi_extra=FieldExtra(editable=True),
    )
    client_id = ma_fields.String(
        attribute="data.client_id",
        required=True,
        bi_extra=FieldExtra(editable=True),
    )
    client_secret = secret_string_field(
        attribute="data.client_secret",
        required=True,
    )
    schema = ma_fields.String(
        attribute="data.schema",
        required=True,
        bi_extra=FieldExtra(editable=True),
    )
    db_name = ma_fields.String(
        attribute="data.db_name",
        required=True,
        bi_extra=FieldExtra(editable=True),
        validate=db_name_no_query_params,
    )
    warehouse = ma_fields.String(
        attribute="data.warehouse",
        required=True,
        bi_extra=FieldExtra(editable=True),
    )
    refresh_token = secret_string_field(
        attribute="data.refresh_token",
        required=False,
    )
    refresh_token_expire_time = ma_fields.DateTime(
        attribute="data.refresh_token_expire_time",
        required=False,
        allow_none=True,
        bi_extra=FieldExtra(editable=True),
    )
