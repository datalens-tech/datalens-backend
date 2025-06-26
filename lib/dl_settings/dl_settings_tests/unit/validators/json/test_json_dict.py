import typing

import pytest

import dl_settings


T = typing.TypeVar("T")


@pytest.mark.parametrize(
    "input, expected",
    [
        ("{}", {}),
        ('{"key": "value"}', {"key": "value"}),
        ('{"key": "value", "key2": "value2"}', {"key": "value", "key2": "value2"}),
    ],
)
def test_function(
    input: str,
    expected: dict[str, str],
) -> None:
    assert dl_settings.parse_json_dict(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("{}", {}),
        ('{"key": "value"}', {"key": "value"}),
        ('{"key": "value", "key2": "value2"}', {"key": "value", "key2": "value2"}),
    ],
)
def test_validator(
    input: str,
    expected: dict[str, str],
) -> None:
    class TestModel(dl_settings.BaseSettings):
        value: typing.Annotated[dict[str, str], dl_settings.json_dict_validator]

    model = TestModel(value=input)  # type: ignore[arg-type]
    assert model.value == expected
