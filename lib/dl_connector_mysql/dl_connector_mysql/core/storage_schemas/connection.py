from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema

from dl_connector_mysql.core.us_connection import ConnectionMySQL


class ConnectionMySQLDataStorageSchema(ConnectionSQLDataStorageSchema[ConnectionMySQL.DataModel]):
    TARGET_CLS = ConnectionMySQL.DataModel

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
