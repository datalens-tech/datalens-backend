from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Dict,
    Mapping,
    cast,
)

from marshmallow import (
    EXCLUDE,
    Schema,
)
from marshmallow import (
    post_dump,
    pre_load,
)
from marshmallow import fields as ma_fields

from dl_constants.enums import UserDataType
from dl_model_tools.schema.base import DefaultSchema
from dl_model_tools.schema.oneofschema import OneOfSchemaWithDumpLoadHooks
from dl_model_tools.typed_values import (
    ArrayFloatValue,
    ArrayIntValue,
    ArrayStrValue,
    BIValue,
    BooleanValue,
    DateTimeTZValue,
    DateTimeValue,
    DateValue,
    FloatValue,
    GenericDateTimeValue,
    GeoPointValue,
    GeoPolygonValue,
    IntegerValue,
    MarkupValue,
    StringValue,
    UuidValue,
)


VALUE_TYPE_CONTEXT_KEY = "bi_value_type"


class WithNestedValueSchema:
    TYPE_FIELD_NAME: ClassVar[str]

    @pre_load(pass_many=False)
    def store_type_in_context(self, data: Mapping[str, Any], **_: Any) -> Mapping[str, Any]:
        if self.TYPE_FIELD_NAME in data:
            cast(Schema, self).context[VALUE_TYPE_CONTEXT_KEY] = data[self.TYPE_FIELD_NAME]
        return data


class ValueSchema(OneOfSchemaWithDumpLoadHooks):
    """
    This schema is designed to dump and load BIValue objects as value itself without any info about its type
    On load it relies on type info stored in context by outer schema object (e.g. see WithNestedValueSchema mixin)
    """

    class Meta:
        unknown = EXCLUDE

    @pre_load(pass_many=False)
    def wrap_value_with_type(self, data: Any, **_: Any) -> Dict[str, Any]:
        type = getattr(self, "context", {}).get(VALUE_TYPE_CONTEXT_KEY)
        return {"type": type, "value": data}

    @post_dump(pass_many=False)
    def extract_value(self, data: Dict[str, Any], **_: Any) -> Any:
        return data["value"]

    class BaseValueSchema(DefaultSchema):
        type = ma_fields.Enum(UserDataType)
        value = ma_fields.Field()

    class StringValueSchema(BaseValueSchema):
        TARGET_CLS = StringValue
        value = ma_fields.String()

    class IntegerValueSchema(BaseValueSchema):
        TARGET_CLS = IntegerValue
        value = ma_fields.Integer()

    class FloatValueSchema(BaseValueSchema):
        TARGET_CLS = FloatValue
        value = ma_fields.Float()

    class DateValueSchema(BaseValueSchema):
        TARGET_CLS = DateValue
        value = ma_fields.Date()

    class DateTimeValueSchema(BaseValueSchema):
        TARGET_CLS = DateTimeValue
        value = ma_fields.DateTime()

    class DateTimeTZValueSchema(BaseValueSchema):
        TARGET_CLS = DateTimeTZValue
        value = ma_fields.DateTime()

    class GenericDateTimeValueSchema(BaseValueSchema):
        TARGET_CLS = GenericDateTimeValue
        value = ma_fields.DateTime()

    class BooleanValueSchema(BaseValueSchema):
        TARGET_CLS = BooleanValue
        value = ma_fields.Boolean()

    class GeoPointValueSchema(BaseValueSchema):
        TARGET_CLS = GeoPointValue
        value = ma_fields.List(ma_fields.Float())

    class GeoPolygonValueSchema(BaseValueSchema):
        TARGET_CLS = GeoPolygonValue
        value = ma_fields.List(ma_fields.List(ma_fields.List(ma_fields.Float())))

    class UuidValueSchema(BaseValueSchema):
        TARGET_CLS = UuidValue
        value = ma_fields.String()

    class MarkupValueSchema(BaseValueSchema):
        TARGET_CLS = MarkupValue
        value = ma_fields.String()

    class ArrayStrValueSchema(BaseValueSchema):
        TARGET_CLS = ArrayStrValue
        value = ma_fields.List(ma_fields.String())

    class ArrayIntValueSchema(BaseValueSchema):
        TARGET_CLS = ArrayIntValue
        value = ma_fields.List(ma_fields.Integer())

    class ArrayFloatValueSchema(BaseValueSchema):
        TARGET_CLS = ArrayFloatValue
        value = ma_fields.List(ma_fields.Float())

    class TreeStrValueSchema(BaseValueSchema):
        TARGET_CLS = ArrayStrValue
        value = ma_fields.List(ma_fields.String())

    type_schemas = {
        UserDataType.string.name: StringValueSchema,
        UserDataType.integer.name: IntegerValueSchema,
        UserDataType.float.name: FloatValueSchema,
        UserDataType.date.name: DateValueSchema,
        UserDataType.datetime.name: DateTimeValueSchema,
        UserDataType.datetimetz.name: DateTimeTZValueSchema,
        UserDataType.genericdatetime.name: GenericDateTimeValueSchema,
        UserDataType.boolean.name: BooleanValueSchema,
        UserDataType.geopoint.name: GeoPointValueSchema,
        UserDataType.geopolygon.name: GeoPolygonValueSchema,
        UserDataType.uuid.name: UuidValueSchema,
        UserDataType.markup.name: MarkupValueSchema,
        UserDataType.array_str.name: ArrayStrValueSchema,
        UserDataType.array_int.name: ArrayIntValueSchema,
        UserDataType.array_float.name: ArrayFloatValueSchema,
        UserDataType.tree_str.name: TreeStrValueSchema,
    }

    def get_obj_type(self, obj: BIValue) -> str:
        return obj.type.name
