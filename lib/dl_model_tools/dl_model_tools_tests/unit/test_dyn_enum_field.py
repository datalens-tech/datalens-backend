from dynamic_enum import (
    AutoEnumValue,
    DynamicEnum,
)
from marshmallow import (
    Schema,
    ValidationError,
)
import pytest

from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


def test_dynamic_enum_field():
    class Color(DynamicEnum):
        blue = AutoEnumValue()

    class ColorSchema(Schema):
        color = DynamicEnumField(Color, attribute="color_attr", allow_none=False)
        other_color = DynamicEnumField(Color, allow_none=True)

    schema = ColorSchema()
    original_data = {"color_attr": Color.blue, "other_color": None}

    serialized_data = schema.dump(original_data)
    assert serialized_data == {"color": "blue", "other_color": None}

    deserialized_data = schema.load(serialized_data)
    assert deserialized_data == original_data

    with pytest.raises(ValidationError):
        schema.dump({"color_attr": "smth", "other_color": None})

    with pytest.raises(ValidationError):
        schema.dump({"color_attr": None, "other_color": None})

    with pytest.raises(ValidationError):
        schema.load({"color_attr": "whatever", "other_color": None})
