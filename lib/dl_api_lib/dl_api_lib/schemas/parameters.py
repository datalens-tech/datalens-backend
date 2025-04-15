from __future__ import annotations

from typing import Any

from marshmallow import EXCLUDE
from marshmallow import fields as ma_fields
from marshmallow_oneofschema import OneOfSchema

from dl_constants.enums import ParameterValueConstraintType
from dl_core.fields import (
    BaseParameterValueConstraint,
    CollectionParameterValueConstraint,
    DefaultParameterValueConstraint,
    EqualsParameterValueConstraint,
    NotEqualsParameterValueConstraint,
    NullParameterValueConstraint,
    RangeParameterValueConstraint,
    RegexParameterValueConstraint,
    SetParameterValueConstraint,
)
from dl_model_tools.schema.base import DefaultSchema
from dl_model_tools.schema.typed_values import ValueSchema


class ParameterValueConstraintSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = "type"

    class BaseParameterValueConstraintSchema(DefaultSchema):
        type = ma_fields.Enum(ParameterValueConstraintType)

    class NullParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = NullParameterValueConstraint

    class RangeParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = RangeParameterValueConstraint

        min = ma_fields.Nested(ValueSchema, allow_none=True)
        max = ma_fields.Nested(ValueSchema, allow_none=True)

    class SetParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = SetParameterValueConstraint

        values = ma_fields.List(ma_fields.Nested(ValueSchema))

    class EqualsParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = EqualsParameterValueConstraint

        value = ma_fields.Nested(ValueSchema)

    class NotEqualsParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = NotEqualsParameterValueConstraint

        value = ma_fields.Nested(ValueSchema)

    class RegexParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = RegexParameterValueConstraint

        pattern = ma_fields.String()

    class DefaultParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = DefaultParameterValueConstraint

    class CollectionParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = CollectionParameterValueConstraint

        # using lambda to avoid circular import in recursive schema
        constraints = ma_fields.List(ma_fields.Nested(lambda: ParameterValueConstraintSchema()))

    type_schemas = {
        ParameterValueConstraintType.null.name: NullParameterValueConstraintSchema,
        ParameterValueConstraintType.range.name: RangeParameterValueConstraintSchema,
        ParameterValueConstraintType.set.name: SetParameterValueConstraintSchema,
        ParameterValueConstraintType.equals.name: EqualsParameterValueConstraintSchema,
        ParameterValueConstraintType.not_equals.name: NotEqualsParameterValueConstraintSchema,
        ParameterValueConstraintType.regex.name: RegexParameterValueConstraintSchema,
        ParameterValueConstraintType.default.name: DefaultParameterValueConstraintSchema,
        ParameterValueConstraintType.collection.name: CollectionParameterValueConstraintSchema,
    }

    def get_obj_type(self, obj: Any) -> str:
        assert isinstance(obj, BaseParameterValueConstraint)
        return obj.type.name
