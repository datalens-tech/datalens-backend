from __future__ import annotations

from typing import Any

from marshmallow import (
    EXCLUDE,
    Schema,
    fields,
    post_load,
)

from dl_constants.enums import UserDataType
from dl_core.db.elements import SchemaColumn
from dl_core.db.native_type import GenericNativeType


class NativeTypeSchema(Schema):
    """(currently used for uploads / ProviderConnection)"""

    class Meta:
        # Same as in `NativeTypeSchemaBase`, to ignore `conn_type`
        unknown = EXCLUDE

    name = fields.String()

    @post_load(pass_many=False)
    def to_object(self, data: dict[str, Any], **kwargs: Any) -> Any:
        if "conn_type" in data:
            # TODO: Remove once all datasets have been migrated
            data = data.copy()
            data.pop("conn_type")
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
