import typing

import pytest

import dl_settings


T = typing.TypeVar("T")


@pytest.mark.parametrize(
    "input, expected",
    [
        ("test", ["test"]),
        ("test,test", ["test", "test"]),
        ("   test,test,    test  ", ["test", "test", "test"]),
    ],
)
def test_function(
    input: str,
    expected: tuple[str, ...],
) -> None:
    assert dl_settings.split_list(",")(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("test", ["test"]),
        ("test,test", ["test", "test"]),
        ("   test,test,    test  ", ["test", "test", "test"]),
    ],
)
def test_validator(
    input: str,
    expected: list[T],
) -> None:
    class TestModel(dl_settings.BaseSettings):
        value: typing.Annotated[list[str], dl_settings.split_list_validator(",")]

    model = TestModel(value=input)  # type: ignore[arg-type]
    assert model.value == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("1,2 ,3 ,4,5", [1, 2, 3, 4, 5]),
    ],
)
def test_validator_with_non_string_type(
    input: str,
    expected: list[int],
) -> None:
    class TestModel(dl_settings.BaseSettings):
        value: typing.Annotated[list[int], dl_settings.split_list_validator(",")]

    model = TestModel(value=input)  # type: ignore[arg-type]
    assert model.value == expected
