from marshmallow import fields as ma_fields

from bi_api_connector.api_schema.connection_base import ConnectionSchema
from bi_api_connector.api_schema.connection_base_fields import (
    cache_ttl_field,
    secret_string_field,
)
from bi_api_connector.api_schema.connection_mixins import RawSQLLevelMixin
from bi_api_connector.api_schema.extras import FieldExtra

from bi_connector_bigquery.core.us_connection import ConnectionSQLBigQuery


class BigQueryConnectionSchema(ConnectionSchema, RawSQLLevelMixin):
    TARGET_CLS = ConnectionSQLBigQuery

    project_id = ma_fields.String(attribute="data.project_id", required=True, bi_extra=FieldExtra(editable=True))
    credentials = secret_string_field(attribute="data.credentials")
    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")
