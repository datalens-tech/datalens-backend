import typing

import dl_settings


def test_function() -> None:
    assert dl_settings.decode_multiline("test") == "test"
    assert dl_settings.decode_multiline("test\\ntest") == "test\ntest"
    assert dl_settings.decode_multiline("test\\n\ntest") == "test\n\ntest"


def test_validator() -> None:
    class TestModel(dl_settings.BaseSettings):
        value: typing.Annotated[str, dl_settings.decode_multiline_validator]

    assert TestModel(value="test").value == "test"
    assert TestModel(value="test\\ntest").value == "test\ntest"
    assert TestModel(value="test\\n\ntest").value == "test\n\ntest"
