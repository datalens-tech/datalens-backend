from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import (
    CacheableConnectionDataSchemaMixin,
    ConnectionBaseDataStorageSchema,
    SubselectConnectionDataSchemaMixin,
)

from dl_connector_chyt.core.us_connection import (
    BaseConnectionCHYT,
    ConnectionCHYTToken,
)


class ConnectionCHYTBaseDataStorageSchema[CHYT_CONN_DATA_TV: BaseConnectionCHYT.DataModel](
    CacheableConnectionDataSchemaMixin,
    SubselectConnectionDataSchemaMixin,
    ConnectionBaseDataStorageSchema[CHYT_CONN_DATA_TV],
):
    alias = ma_fields.String(required=True, allow_none=False)
    max_execution_time = ma_fields.Integer(required=False, allow_none=True, load_default=None, dump_default=None)


class ConnectionCHYTDataStorageSchema(ConnectionCHYTBaseDataStorageSchema[ConnectionCHYTToken.DataModel]):
    TARGET_CLS = ConnectionCHYTToken.DataModel

    token = ma_fields.String(allow_none=False, required=True)
    host = ma_fields.String(allow_none=False, required=True)
    port = ma_fields.Integer(allow_none=False, required=True)
    secure = ma_fields.Boolean(allow_none=False, required=True)
