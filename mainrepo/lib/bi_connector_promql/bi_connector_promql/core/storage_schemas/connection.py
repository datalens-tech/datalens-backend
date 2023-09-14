from marshmallow import fields as ma_fields

from bi_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema

from bi_connector_promql.core.us_connection import PromQLConnection


class PromQLConnectionDataStorageSchema(ConnectionSQLDataStorageSchema[PromQLConnection.DataModel]):
    TARGET_CLS = PromQLConnection.DataModel

    secure = ma_fields.Boolean(required=False, allow_none=False, dump_default=False, load_default=False)
