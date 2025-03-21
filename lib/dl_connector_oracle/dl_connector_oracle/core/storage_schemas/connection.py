from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema

from dl_connector_oracle.core.constants import OracleDbNameType
from dl_connector_oracle.core.us_connection import ConnectionSQLOracle


class ConnectionSQLOracleDataStorageSchema(
    ConnectionSQLDataStorageSchema[ConnectionSQLOracle.DataModel],
):
    TARGET_CLS = ConnectionSQLOracle.DataModel

    db_name_type = ma_fields.Enum(OracleDbNameType, required=True, allow_none=False)
    ssl_enable = ma_fields.Boolean(
        required=False,
        allow_none=False,
        dump_default=False,
        load_default=False,
    )
    ssl_ca = ma_fields.String(
        required=False,
        allow_none=True,
        dump_default=None,
        load_default=None,
    )
