import typing

import pytest

import dl_settings


T = typing.TypeVar("T")


@pytest.mark.parametrize(
    "input, expected",
    [
        (None, None),
        ({"key": "value"}, {"key": "value"}),
        ("{}", {}),
        ('{"key": "value"}', {"key": "value"}),
        ('{"key": "value", "key2": "value2"}', {"key": "value", "key2": "value2"}),
    ],
    ids=[
        "none",
        "dict",
        "empty",
        "one_key",
        "two_keys",
    ],
)
def test_default(
    input: typing.Any,
    expected: dict[str, str],
) -> None:
    assert dl_settings.parse_json_dict(input) == expected

    class TestModel(dl_settings.BaseSettings):
        value: typing.Annotated[dict[str, str] | None, dl_settings.json_dict_validator]

    model = TestModel(value=input)
    assert model.value == expected
