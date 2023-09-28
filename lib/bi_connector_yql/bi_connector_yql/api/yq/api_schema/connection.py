from __future__ import annotations

from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import ConnectionSchema
from dl_api_connector.api_schema.connection_base_fields import (
    cache_ttl_field,
    secret_string_field,
)
from dl_api_connector.api_schema.connection_mixins import RawSQLLevelMixin
from dl_api_connector.api_schema.extras import FieldExtra

from bi_connector_yql.core.yq.us_connection import YQConnection


class YQConnectionSchema(RawSQLLevelMixin, ConnectionSchema):
    TARGET_CLS = YQConnection

    # TODO: require either `folder_id` + `service_account_id`, or `password` # (SA key);
    # or make the `password` (SA key) deprecated.
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
    password = secret_string_field(attribute="data.password", required=False)
    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")
