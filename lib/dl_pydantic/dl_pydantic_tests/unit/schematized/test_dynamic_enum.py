from dynamic_enum import (
    AutoEnumValue,
    DynamicEnum,
)
import pydantic
import pytest

import dl_pydantic


class Color(DynamicEnum):
    red = AutoEnumValue()
    green = AutoEnumValue()
    blue = AutoEnumValue()


class ColorModel(pydantic.BaseModel):
    color: dl_pydantic.SchematizedDynamicEnumAnnotation[Color]


def test_from_instance() -> None:
    model = ColorModel(color=Color.red)
    assert model.color is Color.red


def test_from_string() -> None:
    model = ColorModel(color="red")  # type: ignore[arg-type]
    assert model.color is Color.red


def test_declared_value() -> None:
    YELLOW = Color.declare("yellow")

    model = ColorModel(color="yellow")  # type: ignore[arg-type]
    assert model.color is YELLOW

    model = ColorModel(color=YELLOW)
    assert model.color is YELLOW


def test_invalid_value() -> None:
    with pytest.raises(pydantic.ValidationError):
        ColorModel(color="nonexistent")  # type: ignore[arg-type]


def test_default() -> None:
    class ModelWithDefault(pydantic.BaseModel):
        color: dl_pydantic.SchematizedDynamicEnumAnnotation[Color] = Color.green

    model = ModelWithDefault()
    assert model.color is Color.green


def test_nested() -> None:
    class Inner(pydantic.BaseModel):
        color: dl_pydantic.SchematizedDynamicEnumAnnotation[Color]

    class Outer(pydantic.BaseModel):
        inner: Inner

    model = Outer(inner={"color": "blue"})  # type: ignore[arg-type]
    assert model.inner.color is Color.blue


def test_json_serialization() -> None:
    model = ColorModel(color=Color.red)

    json_data = model.model_dump(mode="json")
    assert json_data == {"color": "red"}

    restored = ColorModel.model_validate(json_data)
    assert restored.color is Color.red


def test_python_serialization() -> None:
    model = ColorModel(color=Color.red)

    python_data = model.model_dump(mode="python")
    assert python_data["color"] is Color.red
