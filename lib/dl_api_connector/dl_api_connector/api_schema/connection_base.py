from __future__ import annotations

from typing import (
    Any,
    ClassVar,
)

from marshmallow import Schema
from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.top_level import USEntryBaseSchema
from dl_constants.enums import (
    ConnectionType,
    DashSQLQueryType,
    UserDataType,
)
from dl_core.us_connection_base import ConnectionBase
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


class ConnectionSchema(USEntryBaseSchema):
    TARGET_CLS: ClassVar[type[ConnectionBase]]
    CONN_TYPE_CTX_KEY: ClassVar[str] = "conn_type"

    # From ConnectionBase.as_dict()
    db_type = DynamicEnumField(ConnectionType, attribute="conn_type", dump_only=True)
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
            ct = ConnectionType(self.context[self.CONN_TYPE_CTX_KEY])
        except ValueError:
            ct = ConnectionType.unknown
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


class RequiredParameterInfoSchema(Schema):
    name = ma_fields.String(dump_only=True)
    data_type = ma_fields.Enum(UserDataType, dump_only=True)


class QueryTypeInfoSchema(Schema):
    query_type = DynamicEnumField(DashSQLQueryType, dump_only=True)
    query_type_label = ma_fields.String(dump_only=True)
    required_parameters = ma_fields.List(ma_fields.Nested(RequiredParameterInfoSchema()), dump_only=True)
    allow_selector = ma_fields.Boolean(dump_only=True)


class ConnectionOptionsSchema(Schema):
    allow_dashsql_usage = ma_fields.Boolean(dump_only=True)
    allow_dataset_usage = ma_fields.Boolean(dump_only=True)
    allow_typed_query_usage = ma_fields.Boolean(dump_only=True)
    query_types = ma_fields.List(ma_fields.Nested(QueryTypeInfoSchema()), dump_only=True)
