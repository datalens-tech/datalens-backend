from marshmallow_enum import EnumField
from marshmallow import fields as ma_fields

from bi_core.us_manager.storage_schemas.connection import (
    ConnectionSQLDataStorageSchema,
    ConnectionMDBStorageDataSchemaMixin,
)

from bi_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode
from bi_connector_postgresql.core.postgresql_base.us_connection import ConnectionPostgreSQLBase


class ConnectionPostgreSQLBaseDataStorageSchema(
        ConnectionMDBStorageDataSchemaMixin,
        ConnectionSQLDataStorageSchema[ConnectionPostgreSQLBase.DataModel],
):
    enforce_collate = EnumField(
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
