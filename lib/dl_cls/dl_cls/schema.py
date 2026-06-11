from typing import Any

from marshmallow import ValidationError
from marshmallow import fields as ma_fields
from marshmallow import validates_schema
from marshmallow.validate import Range

from dl_cls.models import (
    CLSMaskSpec,
    CLSRule,
    CLSSubject,
    FieldCLS,
)
from dl_constants.enums import (
    CLSMode,
    CLSSubjectType,
)
from dl_model_tools.schema.base import DefaultSchema


class CLSMaskSpecSchema(DefaultSchema[CLSMaskSpec]):
    TARGET_CLS = CLSMaskSpec

    mode = ma_fields.Enum(CLSMode, required=True)
    mask = ma_fields.String(load_default="******", dump_default="******")
    # Non-negative: a negative prefix/suffix would slice and leak the source string.
    prefix = ma_fields.Integer(load_default=0, dump_default=0, validate=Range(min=0))
    suffix = ma_fields.Integer(load_default=0, dump_default=0, validate=Range(min=0))
    keep_length = ma_fields.Boolean(load_default=False, dump_default=False)

    @validates_schema
    def validate_mode_params(self, data: dict[str, Any], **kwargs: Any) -> None:
        mode = data.get("mode")
        if mode != CLSMode.partial and (data.get("prefix") or data.get("suffix")):
            raise ValidationError("prefix/suffix are only valid for the 'partial' mode")
        if mode == CLSMode.none and data.get("keep_length"):
            raise ValidationError("keep_length is only valid for the 'partial' and 'full' modes")
        if mode in (CLSMode.partial, CLSMode.full) and not data.get("mask"):
            raise ValidationError("mask must be non-empty for 'partial' and 'full' modes")


class CLSSubjectSchema(DefaultSchema[CLSSubject]):
    TARGET_CLS = CLSSubject

    subject_type = ma_fields.Enum(CLSSubjectType, required=True)
    subject_id = ma_fields.String(required=True)


class CLSRuleSchema(DefaultSchema[CLSRule]):
    TARGET_CLS = CLSRule

    subject = ma_fields.Nested(CLSSubjectSchema, required=True)
    spec = ma_fields.Nested(CLSMaskSpecSchema, required=True)


class FieldCLSSchema(DefaultSchema[FieldCLS]):
    TARGET_CLS = FieldCLS

    default_rule = ma_fields.Nested(CLSMaskSpecSchema, load_default=None)
    rules = ma_fields.List(ma_fields.Nested(CLSRuleSchema), load_default=list)

    @validates_schema
    def validate_default_rule_present(self, data: dict[str, Any], **kwargs: Any) -> None:
        # A configured rule requires an explicit default_rule (no implicit everyone-policy).
        if data.get("rules") and data.get("default_rule") is None:
            raise ValidationError("default_rule is required when rules are present")

    def to_object(self, data: dict[str, Any]) -> FieldCLS:
        # Drop an absent default_rule so the model's safe `full` factory default applies.
        if data.get("default_rule") is None:
            data = {key: value for key, value in data.items() if key != "default_rule"}
        return FieldCLS(**data)
