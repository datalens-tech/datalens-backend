from __future__ import annotations

from typing import Any, Dict

from marshmallow import fields, pre_dump

from bi_constants.enums import IndexKind

from bi_core.connection_executors.models.db_adapter_data import RawColumnInfo, RawSchemaInfo, RawIndexInfo
from bi_core.connection_executors.qe_serializer.schema_base import BaseQEAPISchema
from bi_core.db.native_type_schema import OneOfNativeTypeSchemaBase


class RawColumnInfoSchema(BaseQEAPISchema):
    name = fields.String()
    title = fields.String(allow_none=True)
    nullable = fields.Boolean(allow_none=True)  # deprecated in favor of native_type.nullable
    # Note: using the non-transition schema in the hopes that RQE and app codebases always match.
    native_type = fields.Nested(OneOfNativeTypeSchemaBase)

    def to_object(self, data: Dict[str, Any]) -> RawColumnInfo:
        return RawColumnInfo(**data)


class IndexInfoSchema(BaseQEAPISchema):
    columns = fields.List(fields.String())
    kind = fields.Enum(IndexKind, allow_none=True)
    unique = fields.Boolean()

    def to_object(self, data: Dict[str, Any]) -> RawIndexInfo:
        columns = tuple(data.pop('columns'))
        return RawIndexInfo(columns=columns, **data)


class RawSchemaInfoSchema(BaseQEAPISchema):
    columns = fields.Nested(RawColumnInfoSchema, many=True)
    indexes = fields.Nested(IndexInfoSchema, many=True, allow_none=True)

    def to_object(self, data: Dict[str, Any]) -> RawSchemaInfo:
        return RawSchemaInfo(**data)


class PrimitivesResponseSchema(BaseQEAPISchema):
    value = fields.Raw()

    @pre_dump(pass_many=False)
    def wrap(self, data: Any, **__: Any) -> Dict:
        return {'value': data}

    def to_object(self, data: Dict[str, Any]) -> Any:
        return data['value']
