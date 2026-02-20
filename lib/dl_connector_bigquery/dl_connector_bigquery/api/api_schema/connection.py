from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import ConnectionSchema
from dl_api_connector.api_schema.connection_base_fields import secret_string_field
from dl_api_connector.api_schema.connection_mixins import (
    QueryCacheMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.extras import FieldExtra

from dl_connector_bigquery.core.us_connection import ConnectionSQLBigQuery


class BigQueryConnectionSchema(ConnectionSchema, RawSQLLevelMixin, QueryCacheMixin):
    TARGET_CLS = ConnectionSQLBigQuery

    project_id = ma_fields.String(attribute="data.project_id", required=True, bi_extra=FieldExtra(editable=True))
    credentials = secret_string_field(attribute="data.credentials")
