from __future__ import annotations

from bi_formula.core.datatype import (
    DataType,
    DataTypeParams,
)


class TypeStrategy:
    __slots__ = ()

    def get_from_args(self, arg_types) -> DataType:
        raise NotImplementedError


class TypeParamsStrategy:
    __slots__ = ()

    def get_from_arg_values(self, args) -> DataTypeParams:
        raise NotImplementedError
