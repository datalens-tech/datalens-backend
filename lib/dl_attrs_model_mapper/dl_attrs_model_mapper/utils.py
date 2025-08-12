import collections
import typing
from typing import (
    Any,
    Literal,
    Optional,
    Sequence,
    Union,
)

import attr

from dl_attrs_model_mapper.structs.mappings import FrozenStrMapping


Locale = Union[Literal["ru"], Literal["en"]]


@attr.s(frozen=True)
class MText:
    ru: str = attr.ib()
    en: Optional[str] = attr.ib(default=None)

    def at_locale(self, locale: Locale) -> Optional[str]:
        if locale == "ru":
            return self.ru
        if locale == "en":
            return self.en
        raise ValueError(f"Unknown locale {locale!r}")


class CommonMAFieldKWArgs(typing.TypedDict):
    required: bool
    allow_none: bool
    attribute: Optional[str]
    load_only: bool


@attr.s(frozen=True, auto_attribs=True)
class CommonAttributeProps:
    required: bool
    allow_none: bool
    attribute_name: Optional[str]
    load_only: bool
    description: Optional[MText]

    def to_common_ma_field_kwargs(self) -> CommonMAFieldKWArgs:
        return dict(
            required=self.required,
            allow_none=self.allow_none,
            attribute=self.attribute_name,
            load_only=self.load_only,
        )


def is_sequence(container_type: Any) -> bool:
    return container_type in (list, Sequence, collections.abc.Sequence, list)


def is_str_mapping(container_type: Any) -> bool:
    return container_type is FrozenStrMapping


def unwrap_typing_container_with_single_type(the_type: Any) -> tuple[Any, type]:
    assert the_type is not None

    origin = typing.get_origin(the_type)

    effective_origin: Any
    nested_types: set[type]

    if origin == Union:
        nested_types = set(typing.get_args(the_type))

        if type(None) in nested_types:
            nested_types.remove(type(None))
            effective_origin = Optional
        else:
            raise ValueError("Unions are not supported")

    elif is_sequence(origin) or is_str_mapping(origin):
        nested_types = set(typing.get_args(the_type))
        effective_origin = origin

    else:
        nested_types = {the_type}
        effective_origin = None

    if len(nested_types) != 1:
        raise ValueError("Multiple value in container types is not supported")

    return effective_origin, next(iter(nested_types))


def unwrap_container_stack_with_single_type(the_type: Any) -> tuple[Sequence[Any], type]:
    container_stack: list[Any] = []

    next_type: Any = the_type

    while True:
        container_type, effective_type = unwrap_typing_container_with_single_type(next_type)
        if container_type is None:
            return tuple(container_stack), effective_type

        container_stack.append(container_type)
        next_type = effective_type


def ensure_tuple(col: Optional[Sequence]) -> Optional[tuple]:
    if col is None:
        return None
    if isinstance(col, tuple):
        return col
    if isinstance(col, list):
        return tuple(col)
    else:
        raise TypeError()


def ensure_tuple_of_tuples(col: Optional[Sequence[Sequence]]) -> Optional[tuple[Optional[tuple], ...]]:
    if col is None:
        return None
    if isinstance(col, tuple) and all(isinstance(sub_col, tuple) for sub_col in col):
        return col
    if isinstance(col, (list, tuple)):
        return tuple(sub_col if isinstance(sub_col, tuple) else tuple(sub_col) for sub_col in col)
    else:
        raise TypeError()
