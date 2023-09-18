from marshmallow import fields as ma_fields

from dl_constants.enums import (
    ComponentErrorLevel,
    ComponentType,
)
from dl_core import component_errors
from dl_core.marshmallow import ErrorCodeField
from dl_model_tools.schema.base import DefaultSchema


class ComponentErrorListSchema(DefaultSchema[component_errors.ComponentErrorRegistry]):
    TARGET_CLS = component_errors.ComponentErrorRegistry

    class ComponentErrorPackSchema(DefaultSchema[component_errors.ComponentErrorPack]):
        TARGET_CLS = component_errors.ComponentErrorPack

        class ComponentErrorSchema(DefaultSchema[component_errors.ComponentError]):
            TARGET_CLS = component_errors.ComponentError

            message = ma_fields.String()
            level = ma_fields.Enum(ComponentErrorLevel)
            code = ErrorCodeField()
            details = ma_fields.Dict()

        id = ma_fields.String()
        type = ma_fields.Enum(ComponentType)
        errors = ma_fields.List(ma_fields.Nested(ComponentErrorSchema))

    items = ma_fields.List(ma_fields.Nested(ComponentErrorPackSchema))
