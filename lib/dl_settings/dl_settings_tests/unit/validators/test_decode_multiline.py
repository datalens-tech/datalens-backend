import typing

import pydantic

import dl_settings
import dl_settings.validators as dl_settings_validators


def test_function() -> None:
    assert dl_settings_validators.decode_multiline("test") == "test"
    assert dl_settings_validators.decode_multiline("test\\ntest") == "test\ntest"
    assert dl_settings_validators.decode_multiline("test\\n\ntest") == "test\n\ntest"


def test_model() -> None:
    class TestModel(dl_settings.BaseSettings):
        value: typing.Annotated[str, pydantic.BeforeValidator(dl_settings_validators.decode_multiline)]

    assert TestModel(value="test").value == "test"
    assert TestModel(value="test\\ntest").value == "test\ntest"
    assert TestModel(value="test\\n\ntest").value == "test\n\ntest"
