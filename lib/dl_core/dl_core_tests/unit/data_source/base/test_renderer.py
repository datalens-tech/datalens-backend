import pytest

from dl_core.data_source.base import DataSourceRenderer
from dl_core.exc import TemplateInvalidError
from dl_model_tools.typed_values import (
    BooleanValue,
    FloatValue,
    IntegerValue,
    StringValue,
)


@pytest.fixture
def renderer() -> DataSourceRenderer:
    return DataSourceRenderer(
        values={
            "string_key": StringValue(value="test_value"),
            "integer_key": IntegerValue(value=42),
            "float_key": FloatValue(value=3.14),
            "boolean_key": BooleanValue(value=True),
        }
    )


def test_default(renderer: DataSourceRenderer):
    assert renderer.render("test") == "test"
    assert renderer.render("{{string_key}}") == "test_value"
    assert renderer.render("{{integer_key}}") == "42"
    assert renderer.render("{{float_key}}") == "3.14"
    assert renderer.render("{{boolean_key}}") == "True"


def test_with_spaces(renderer: DataSourceRenderer):
    assert renderer.render("{{  string_key}}") == "test_value"
    assert renderer.render("{{string_key  }}") == "test_value"
    assert renderer.render("{{             string_key}}") == "test_value"
    assert renderer.render("{{  string_key   }}") == "test_value"


def test_with_invalid_values(renderer: DataSourceRenderer):
    with pytest.raises(TemplateInvalidError):
        renderer.render("{{test_another_key}}")


def test_nested(renderer: DataSourceRenderer):
    with pytest.raises(TemplateInvalidError):
        renderer.render("{{ {{string_key}} }}")


def test_single_opening_bracket(renderer: DataSourceRenderer):
    with pytest.raises(TemplateInvalidError):
        renderer.render("test{")

    with pytest.raises(TemplateInvalidError):
        renderer.render("test{string_key")


def test_single_closing_bracket(renderer: DataSourceRenderer):
    with pytest.raises(TemplateInvalidError):
        renderer.render("{{string_key}")

    with pytest.raises(TemplateInvalidError):
        renderer.render("{{string_key}test")


def test_unmatched_brackets(renderer: DataSourceRenderer):
    with pytest.raises(TemplateInvalidError):
        renderer.render("{{string_key")
