from __future__ import annotations

from typing import Any

from marshmallow import (
    Schema,
    fields,
    post_load,
)

from dl_constants.enums import (
    ConnectionType,
    UserDataType,
)
from dl_core.db.elements import SchemaColumn
from dl_core.db.native_type import GenericNativeType
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


class NativeTypeSchema(Schema):
    """(currently used for uploads / ProviderConnection)"""

    conn_type = DynamicEnumField(ConnectionType, by_value=True, load_default=ConnectionType.unknown)
    name = fields.String()

    @post_load(pass_many=False)
    def to_object(self, data: dict[str, Any], **kwargs: Any) -> Any:
        return GenericNativeType(**data)


class SchemaColumnStorageSchema(Schema):
    """(currently used for uploads / ProviderConnection)"""

    name = fields.String(allow_none=False)
    title = fields.String(allow_none=True)
    user_type = fields.Enum(UserDataType, by_value=False)
    nullable = fields.Boolean()
    native_type = fields.Nested(NativeTypeSchema, allow_none=True)
    source_id = fields.String(allow_none=True)

    @post_load(pass_many=False)
    def to_object(self, data: dict[str, Any], **kwargs: Any) -> Any:
        return SchemaColumn(**data)
