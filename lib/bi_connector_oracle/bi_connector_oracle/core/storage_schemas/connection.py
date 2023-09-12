from marshmallow import fields as ma_fields

from bi_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema

from bi_connector_oracle.core.constants import OracleDbNameType
from bi_connector_oracle.core.us_connection import ConnectionSQLOracle


class ConnectionSQLOracleDataStorageSchema(
        ConnectionSQLDataStorageSchema[ConnectionSQLOracle.DataModel],
):
    TARGET_CLS = ConnectionSQLOracle.DataModel

    db_name_type = ma_fields.Enum(OracleDbNameType, required=True, allow_none=False)
