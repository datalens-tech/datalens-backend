from __future__ import annotations

from typing import (
    Any,
    ClassVar,
)

from marshmallow import Schema
from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.top_level import USEntryBaseSchema
from dl_constants.enums import ConnectionType as CT
from dl_constants.enums import DashSQLQueryType
from dl_core.us_connection_base import ConnectionBase
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


class ConnectionSchema(USEntryBaseSchema):
    TARGET_CLS = ConnectionBase
    CONN_TYPE_CTX_KEY: ClassVar[str] = "conn_type"

    # From ConnectionBase.as_dict()
    db_type = DynamicEnumField(CT, attribute="conn_type", dump_only=True)
    # TODO FIX: Change for datetime field after created/updated _at become datetime instead of string
    created_at = ma_fields.String(dump_only=True)
    updated_at = ma_fields.String(dump_only=True)

    # Create only fields
    permissions_mode = ma_fields.Raw(load_default=None, load_only=True)
    initial_permissions = ma_fields.Raw(load_default=None, load_only=True)

    def create_data_model_constructor_kwargs(self, data_attributes: dict[str, Any]) -> dict[str, Any]:
        """
        Point for adding extra kwargs for DataModel constructor:
         - Fields that is not defined in schema
         - Context-dependant defaults
         - Required fields defaults for fields which was turned into partials
        """
        return dict(data_attributes)

    def create_data_model(self, data_attributes: dict[str, Any]) -> Any:
        return self.TARGET_CLS.DataModel(**self.create_data_model_constructor_kwargs(data_attributes))

    def default_create_from_dict_kwargs(self, data: dict[str, Any]) -> dict[str, Any]:
        try:
            ct = CT(self.context[self.CONN_TYPE_CTX_KEY])
        except ValueError:
            ct = CT.unknown
        ret = dict(
            super().default_create_from_dict_kwargs(data),
            type_=ct.name,
            permissions_mode=data["permissions_mode"],
            initial_permissions=data["initial_permissions"],
        )

        return ret


class ConnectionMetaMixin(ConnectionSchema):
    """
    This is a base class for Schematic-based connections
    To be removed after migration to attrs
    """

    meta = ma_fields.Dict(dump_only=True)  # In ConnectionBase.as_dict() meta was included


class DashSQLQueryTypeInfo(Schema):
    dashsql_query_type = DynamicEnumField(DashSQLQueryType, dump_only=True)
    dashsql_query_type_label = ma_fields.String(dump_only=True)


class ConnectionOptionsSchema(Schema):
    allow_dashsql_usage = ma_fields.Boolean(dump_only=True)
    allow_dataset_usage = ma_fields.Boolean(dump_only=True)
    dashsql_query_types = ma_fields.Nested(DashSQLQueryTypeInfo(), dump_only=True, many=True)
