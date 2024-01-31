from __future__ import annotations

from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    cast,
)

from dl_formula.core.datatype import DataType
from dl_formula_ref.registry.arg_base import (
    ArgumentExtractorBase,
    FuncArg,
)


if TYPE_CHECKING:
    import dl_formula_ref.registry.base as _registry_base
    from dl_formula_ref.registry.env import GenerationEnvironment


INFINITE_ARG_COUNT = 3


def _get_expandable_types() -> dict[DataType, set[DataType]]:
    """
    Generate a mapping of types that other non-const types can autocast to
    (e.g. {DataType.FLOAT: {DataType.INTEGER}, ...}).

    This is needed to expand all suitable types in function signatures
    if they are not listed explicitly.
    """

    result: dict[DataType, set[DataType]] = {}
    for dtype in DataType:
        if dtype.is_const:
            continue
        autocast_dtypes = cast(
            set[DataType],
            {
                actype
                for actype in dtype.autocast_types
                if actype is not dtype and not actype.is_const and actype is not DataType.NULL
            },
        )
        if autocast_dtypes:
            result[dtype] = autocast_dtypes

    return result


EXPANDABLE_TYPES: dict[DataType, set[DataType]] = _get_expandable_types()


class DefaultArgumentExtractor(ArgumentExtractorBase):
    def get_args(self, item: _registry_base.FunctionDocRegistryItem, env: GenerationEnvironment) -> list[FuncArg]:
        def_list = item.get_implementation_specs(env=env)
        min_arg_cnt: int
        max_arg_cnt: int
        if any(defn.arg_cnt is None for defn in def_list):
            arg_names = max((defn.arg_names or () for defn in def_list), key=lambda names: len(names))
            min_arg_cnt = max(len(arg_names), INFINITE_ARG_COUNT)
            max_arg_cnt = max(len(arg_names), INFINITE_ARG_COUNT)
        else:
            min_arg_cnt = min(defn.arg_cnt for defn in def_list)  # type: ignore  # 2024-01-30 # TODO: Value of type variable "SupportsRichComparisonT" of "min" cannot be "int | None"  [type-var]
            max_arg_cnt = max(defn.arg_cnt for defn in def_list)  # type: ignore  # 2024-01-30 # TODO: Value of type variable "SupportsRichComparisonT" of "max" cannot be "int | None"  [type-var]
            arg_names = max((defn.arg_names or () for defn in def_list), key=lambda names: len(names))

        # generate arg types
        arg_type_lists: dict[int, set[DataType]] = defaultdict(set)  # {<ind>: [<possible types>]}
        for defn in def_list:
            tr_arg_cnt = defn.arg_cnt if defn.arg_cnt is not None else max_arg_cnt
            for arg_matcher in defn.argument_types or ():
                # each variant has its own possible set of arg types
                for ind in range(tr_arg_cnt):
                    poss_types = arg_matcher.get_possible_arg_types_at_pos(ind, total=tr_arg_cnt)
                    arg_type_lists[ind] |= poss_types

                    for arg_type in poss_types:
                        # do not show const types if non-const type is accepted for this argument
                        const_type = arg_type.const_type
                        non_const_type = arg_type.non_const_type
                        if const_type in arg_type_lists[ind] and non_const_type in arg_type_lists[ind]:
                            arg_type_lists[ind].remove(const_type)

        # generate arg names if they are not defined
        if not arg_names:  # single anonymous arg
            if max_arg_cnt == 1:
                arg_names = ["value"]
            else:  # multiple anonymous args
                arg_names = ["arg_{}".format(i + 1) for i in range(max_arg_cnt)]

        # patch all type sets with implicit INTEGERs if FLOAT is accepted
        for types in arg_type_lists.values():
            for dtype in types.copy():
                if dtype in EXPANDABLE_TYPES:
                    types |= EXPANDABLE_TYPES[dtype]

        return [
            FuncArg(
                name=arg_names[i],
                types=arg_type_lists[i],
                optional_level=max(0, i + 1 - min_arg_cnt),
            )
            for i in range(max_arg_cnt)
        ]
