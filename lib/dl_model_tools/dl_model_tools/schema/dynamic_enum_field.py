from typing import (
    Any,
    Mapping,
    Optional,
)

from dynamic_enum import DynamicEnum
from marshmallow import ValidationError
from marshmallow import fields as ma_fields


class DynamicEnumField(ma_fields.Field):
    def __init__(self, dyn_enum_cls: type[DynamicEnum], **kwargs: Any):
        super().__init__(**kwargs)
        self._dyn_enum_cls = dyn_enum_cls

    def _serialize(self, value: Any, attr: Optional[str], obj: Any, **kwargs: Any) -> Optional[str]:
        if value is None and self.allow_none:
            return None

        if not isinstance(value, self._dyn_enum_cls):
            raise ValidationError(f"Invalid type {type(value).__name__} for {attr}")

        return value.value

    def _deserialize(self, value: Any, attr: Optional[str], data: Optional[Mapping[str, Any]], **kwargs: Any) -> Any:
        if value is None and self.allow_none:
            return None

        if not isinstance(value, str):
            raise ValidationError(f"Invalid type {type(value).__name__} for {attr}")

        if not self._dyn_enum_cls.is_declared(value):
            raise ValidationError(f"Invalid value {value} for {attr}")

        deserialized_value = self._dyn_enum_cls(value)
        return deserialized_value
