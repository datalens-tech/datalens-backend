from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    Type,
)

from dl_core.db.native_type import (
    GenericNativeType,
    LengthedNativeType,
)


if TYPE_CHECKING:
    from sqlalchemy.types import TypeEngine

    TypeFactory = Callable[[GenericNativeType], TypeEngine]


make_native_type = GenericNativeType.normalize_name_and_create


def simple_instantiator(typecls: Type[TypeEngine]) -> TypeFactory:
    def type_gen(nt: GenericNativeType) -> TypeEngine:
        return typecls()

    return type_gen


def lengthed_instantiator(typecls: Type[TypeEngine], default_length: int = 255) -> TypeFactory:
    def type_gen(nt: GenericNativeType) -> TypeEngine:
        if isinstance(nt, LengthedNativeType):
            length = nt.length or default_length
        else:
            length = default_length
        return typecls(length)  # type: ignore  # TODO: fix

    return type_gen


def timezone_instantiator(typecls: Type[TypeEngine]) -> TypeFactory:
    def type_gen(nt: GenericNativeType) -> TypeEngine:
        return typecls(timezone=True)  # type: ignore  # TODO: fix

    return type_gen


def typed_instantiator(typecls: Type[TypeEngine], inner_type: Type[TypeEngine]) -> TypeFactory:
    def type_gen(nt: GenericNativeType) -> TypeEngine:
        return typecls(inner_type)  # type: ignore  # TODO: fix

    return type_gen
