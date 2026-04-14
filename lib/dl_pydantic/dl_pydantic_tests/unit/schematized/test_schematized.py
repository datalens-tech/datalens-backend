import pydantic
import pytest

from dl_dynamic_enum import (
    AutoEnumValue,
    DynamicEnum,
)
import dl_pydantic


class Color(DynamicEnum):
    red = AutoEnumValue()
    green = AutoEnumValue()
    blue = AutoEnumValue()


def test_dispatches_to_dynamic_enum() -> None:
    class Model(pydantic.BaseModel):
        color: dl_pydantic.SchematizedAnnotation[Color]

    model = Model(color=Color.red)
    assert model.color is Color.red

    model = Model(color="blue")  # type: ignore[arg-type]
    assert model.color is Color.blue


def test_rejects_unsupported_type() -> None:
    with pytest.raises(TypeError, match="No schematized annotation registered"):
        dl_pydantic.SchematizedAnnotation[str]
