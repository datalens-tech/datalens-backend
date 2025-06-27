import typing

import pytest

import dl_settings


T = typing.TypeVar("T")


@pytest.mark.parametrize(
    "input, expected",
    [
        (None, None),
        ("test", ["test"]),
        ("test,test", ["test", "test"]),
        ("   test,test,    test  ", ["test", "test", "test"]),
        ("1,2 ,3 ,4,5", ["1", "2", "3", "4", "5"]),
        (["1", "2", "3"], ["1", "2", "3"]),
    ],
    ids=[
        "none",
        "str",
        "str_comma",
        "str_comma_spaces",
        "str_comma_spaces_int",
        "list",
    ],
)
def test_default(
    input: typing.Any,
    expected: list[str],
) -> None:
    assert dl_settings.split_list(",")(input) == expected

    class TestModel(dl_settings.BaseSettings):
        value: typing.Annotated[list[str] | None, dl_settings.split_list_validator(",")]

    model = TestModel(value=input)
    assert model.value == expected
