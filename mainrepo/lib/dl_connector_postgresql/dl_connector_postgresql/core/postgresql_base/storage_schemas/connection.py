from marshmallow import fields as ma_fields

from dl_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode
from dl_connector_postgresql.core.postgresql_base.us_connection import ConnectionPostgreSQLBase
from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema


class ConnectionPostgreSQLBaseDataStorageSchema(
    ConnectionSQLDataStorageSchema[ConnectionPostgreSQLBase.DataModel],
):
    enforce_collate = ma_fields.Enum(
        PGEnforceCollateMode,
        required=False,
        allow_none=False,
        dump_default=PGEnforceCollateMode.auto,
        load_default=PGEnforceCollateMode.auto,
    )
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
