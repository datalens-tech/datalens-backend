from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema

from dl_connector_trino.core.constants import TrinoAuthType
from dl_connector_trino.core.us_connection import ConnectionTrino


class TrinoConnectionDataStorageSchema(ConnectionSQLDataStorageSchema[ConnectionTrino.DataModel]):
    TARGET_CLS = ConnectionTrino.DataModel

    auth_type = ma_fields.Enum(
        TrinoAuthType,
        required=True,
        allow_none=False,
    )
    ssl_enable = ma_fields.Boolean(
        required=False,
        allow_none=False,
        dump_default=True,
        load_default=False,
    )
    ssl_ca = ma_fields.String(
        required=False,
        allow_none=True,
        dump_default=None,
        load_default=None,
    )
    jwt = ma_fields.String(
        required=False,
        allow_none=True,
        dump_default=None,
        load_default=None,
    )
