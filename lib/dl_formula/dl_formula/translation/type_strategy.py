from __future__ import annotations

from dl_formula.core.datatype import (
    DataType,
    DataTypeParams,
)


class TypeStrategy:
    __slots__ = ()

    def get_from_args(self, arg_types) -> DataType:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        raise NotImplementedError


class TypeParamsStrategy:
    __slots__ = ()

    def get_from_arg_values(self, args) -> DataTypeParams:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        raise NotImplementedError
