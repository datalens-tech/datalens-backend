from marshmallow import fields as ma_fields

from bi_connector_chyt.core.storage_schemas.connection import ConnectionCHYTBaseDataStorageSchema
from bi_connector_chyt_internal.core.us_connection import (
    BaseConnectionCHYTInternal,
    ConnectionCHYTInternalToken,
    ConnectionCHYTUserAuth,
)


class ConnectionCHYTInternalBaseDataStorageSchema(
    ConnectionCHYTBaseDataStorageSchema[BaseConnectionCHYTInternal.DataModel],
):
    cluster = ma_fields.String(required=True, allow_none=False)


class ConnectionCHYTInternalTokenDataStorageSchema(ConnectionCHYTInternalBaseDataStorageSchema):
    TARGET_CLS = ConnectionCHYTInternalToken.DataModel

    token = ma_fields.String(allow_none=False, required=True)


class ConnectionCHYTUserAuthDataStorageSchema(ConnectionCHYTInternalBaseDataStorageSchema):
    TARGET_CLS = ConnectionCHYTUserAuth.DataModel
