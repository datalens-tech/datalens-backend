import typing

import pydantic


def _split(value: str, separator: str) -> list[str]:
    return [entry.strip() for entry in value.split(separator) if entry]


def split_list(separator: str) -> typing.Callable[[typing.Any], list[str] | None]:
    def validator(value: typing.Any) -> list[str] | None:
        if value is None:
            return None

        if isinstance(value, list):
            return value

        if isinstance(value, str):
            return _split(value, separator)

        raise ValueError(f"Invalid value: {value}")

    return validator


def split_tuple(separator: str) -> typing.Callable[[typing.Any], tuple[str, ...] | None]:
    def validator(value: typing.Any) -> tuple[str, ...] | None:
        if value is None:
            return None

        if isinstance(value, tuple):
            return value

        if isinstance(value, str):
            return tuple(_split(value, separator))

        raise ValueError(f"Invalid value: {value}")

    return validator


def split_list_validator(separator: str) -> pydantic.BeforeValidator:
    return pydantic.BeforeValidator(split_list(separator))


def split_tuple_validator(separator: str) -> pydantic.BeforeValidator:
    return pydantic.BeforeValidator(split_tuple(separator))


__all__ = [
    "split_list",
    "split_tuple",
]
