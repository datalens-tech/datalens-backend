from __future__ import annotations

from typing import Any

from marshmallow import EXCLUDE
from marshmallow import fields as ma_fields
from marshmallow_oneofschema import OneOfSchema

from dl_constants.enums import ParameterValueConstraintType
from dl_core.fields import (
    AllParameterValueConstraint,
    BaseParameterValueConstraint,
    RangeParameterValueConstraint,
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

    class AllParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = AllParameterValueConstraint

    class RangeParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = RangeParameterValueConstraint

        min = ma_fields.Nested(ValueSchema, allow_none=True)
        max = ma_fields.Nested(ValueSchema, allow_none=True)

    class SetParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
        TARGET_CLS = SetParameterValueConstraint

        values = ma_fields.List(ma_fields.Nested(ValueSchema))

    type_schemas = {
        ParameterValueConstraintType.all.name: AllParameterValueConstraintSchema,
        ParameterValueConstraintType.range.name: RangeParameterValueConstraintSchema,
        ParameterValueConstraintType.set.name: SetParameterValueConstraintSchema,
    }

    def get_obj_type(self, obj: Any) -> str:
        assert isinstance(obj, BaseParameterValueConstraint)
        return obj.type.name
