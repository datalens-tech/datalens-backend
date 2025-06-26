from typing import Callable

import pydantic


def _split(value: str, separator: str) -> list[str]:
    return [entry.strip() for entry in value.split(separator) if entry]


def split_list(separator: str) -> Callable[[str], list[str]]:
    def validator(value: str) -> list[str]:
        return _split(value, separator)

    return validator


def split_tuple(separator: str) -> Callable[[str], tuple[str, ...]]:
    def validator(value: str) -> tuple[str, ...]:
        return tuple(_split(value, separator))

    return validator


def split_list_validator(separator: str) -> pydantic.BeforeValidator:
    return pydantic.BeforeValidator(split_list(separator))


def split_tuple_validator(separator: str) -> pydantic.BeforeValidator:
    return pydantic.BeforeValidator(split_tuple(separator))


__all__ = [
    "split_list",
    "split_tuple",
]
