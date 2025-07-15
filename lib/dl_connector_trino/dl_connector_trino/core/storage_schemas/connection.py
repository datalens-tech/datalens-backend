from marshmallow import Schema
from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField

from dl_connector_trino.core.constants import (
    ListingSources,
    TrinoAuthType,
)
from dl_connector_trino.core.us_connection import ConnectionTrino


class TrinoConnectionDataStorageSchemaBase(Schema):
    auth_type = DynamicEnumField(
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
    listing_sources = DynamicEnumField(
        ListingSources,
        required=False,
        allow_none=False,
        dump_default=ListingSources.on,
        load_default=ListingSources.on,
    )


class TrinoConnectionDataStorageSchema(
    ConnectionSQLDataStorageSchema[ConnectionTrino.DataModel],
    TrinoConnectionDataStorageSchemaBase,
):
    TARGET_CLS = ConnectionTrino.DataModel
