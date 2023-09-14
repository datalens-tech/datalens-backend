from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    List,
    Sequence,
    Union,
)

from bi_formula.core.datatype import (
    DataType,
    DataTypeParams,
)
import bi_formula.core.exc as exc
from bi_formula.translation.type_strategy import (
    TypeParamsStrategy,
    TypeStrategy,
)

if TYPE_CHECKING:
    from bi_formula.translation.context import TranslationCtx


class Fixed(TypeStrategy):
    __slots__ = ("_type",)

    def __init__(self, type_: DataType):
        self._type = type_

    @property
    def type(self) -> DataType:
        return self._type

    def get_from_args(self, arg_types: List[DataType]) -> DataType:
        return self._type


class FromArgs(TypeStrategy):
    __slots__ = ("_indices",)

    def __init__(self, *indices: Union[int, slice]):
        self._indices = indices or [slice(0, None)]

    def get_from_args(self, arg_types: List[DataType]) -> DataType:
        return self._get_from_args_and_indices(arg_types, self._indices)  # type: ignore

    @staticmethod
    def _get_from_args_and_indices(
        arg_types: List[DataType],
        indices: Sequence[Union[int, slice]],
    ) -> DataType:
        use_arg_types = []
        for ind in indices:  # type: ignore
            if isinstance(ind, int):
                use_arg_types.append(arg_types[ind])
            elif isinstance(ind, slice):
                use_arg_types.extend(arg_types[ind])
            else:
                raise TypeError(type(ind))

        return DataType.get_common_cast_type(*use_arg_types).non_const_type


class DynamicIndexStrategy(FromArgs):
    __slots__ = ()

    def get_indices(self, arg_cnt: int) -> List[Union[int, slice]]:
        return list(range(arg_cnt))

    def get_from_args(self, arg_types: List[DataType]) -> DataType:
        return self._get_from_args_and_indices(arg_types, self.get_indices(len(arg_types))).non_const_type


class Combined(TypeStrategy):
    __slots__ = ("_substrats", "_replace_types")

    def __init__(self, *substrats: TypeStrategy, replace_types=None):
        self._substrats = []
        self._replace_types = replace_types or {}
        for substrat in substrats or []:
            if isinstance(substrat, DataType):
                substrat = Fixed(substrat)
            self._substrats.append(substrat)

    def get_from_args(self, arg_types: List[DataType]) -> DataType:
        types = set()
        for substrat in self._substrats:
            types.add(substrat.get_from_args(arg_types))

        for repl_t, new_t in self._replace_types.items():
            if repl_t in types:
                types.remove(repl_t)
                types.add(new_t)

        return DataType.get_common_cast_type(*types)


class CaseTypeStrategy(DynamicIndexStrategy):
    __slots__ = ()

    def get_indices(self, arg_cnt: int) -> List[Union[int, slice]]:
        if arg_cnt < 2 or arg_cnt % 2 != 0:
            raise exc.TranslationError(f"Invalid number of arguments for CASE: {arg_cnt}")
        num_of_whens = (arg_cnt - 1) // 2  # 1 for main CASE value arg, 2 for each WHEN part
        return [*[2 + 2 * i for i in range(num_of_whens)], 1 + 2 * num_of_whens]  # THENs  # ELSE


class IfTypeStrategy(DynamicIndexStrategy):
    __slots__ = ()

    def get_indices(self, arg_cnt: int) -> List[Union[int, slice]]:
        if arg_cnt < 3 or arg_cnt % 2 != 1:
            raise exc.TranslationError(f"Invalid number of arguments for IF: {arg_cnt}")
        num_of_if_parts = arg_cnt // 2
        return [*[2 * i + 1 for i in range(num_of_if_parts)], 2 * num_of_if_parts]  # THENs  # ELSE


class ParamsEmpty(TypeParamsStrategy):
    def get_from_arg_values(self, args: List[TranslationCtx]) -> DataTypeParams:
        return DataTypeParams()


class ParamsCustom(TypeParamsStrategy):
    def __init__(self, func: Callable[[List[TranslationCtx]], DataTypeParams]):
        self._func = func

    def get_from_arg_values(self, args: List[TranslationCtx]) -> DataTypeParams:
        return self._func(args)


class ParamsFromArgs(TypeParamsStrategy):
    def __init__(self, index: int):
        self._index = index

    def get_from_arg_values(self, args: List[TranslationCtx]) -> DataTypeParams:
        return args[self._index].data_type_params  # type: ignore
