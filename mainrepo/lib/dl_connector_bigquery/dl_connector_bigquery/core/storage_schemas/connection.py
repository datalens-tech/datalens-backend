from marshmallow import fields as ma_fields

from dl_connector_bigquery.core.us_connection import ConnectionSQLBigQuery
from dl_core.us_manager.storage_schemas.connection import (
    BaseConnectionDataStorageSchema,
    CacheableConnectionDataSchemaMixin,
    SubselectConnectionDataSchemaMixin,
)


class BigQueryConnectionDataStorageSchema(
    BaseConnectionDataStorageSchema[ConnectionSQLBigQuery.DataModel],
    CacheableConnectionDataSchemaMixin,
    SubselectConnectionDataSchemaMixin,
):
    TARGET_CLS = ConnectionSQLBigQuery.DataModel

    project_id = ma_fields.String(allow_none=False, required=True)
    credentials = ma_fields.String(allow_none=False, required=True)
