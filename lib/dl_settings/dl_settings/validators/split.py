import typing

import pydantic


def _get_splitter(separator: str) -> typing.Callable[[typing.Any], typing.Any]:
    def splitter(value: typing.Any) -> typing.Any:
        if isinstance(value, str):
            return [entry.strip() for entry in value.split(separator) if entry]

        return value

    return splitter


def split_validator(separator: str) -> pydantic.BeforeValidator:
    return pydantic.BeforeValidator(_get_splitter(separator))


__all__ = [
    "split_validator",
]
