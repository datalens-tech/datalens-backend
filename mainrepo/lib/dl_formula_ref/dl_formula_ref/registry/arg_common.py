from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Union,
)

from dl_formula.definitions.type_strategy import (
    DynamicIndexStrategy,
    Fixed,
    FromArgs,
    TypeStrategy,
)
from dl_formula_ref.registry.text import ParameterizedText
from dl_formula_ref.texts import (
    COMMON_TYPE_NOTE,
    DEPENDS_ON_ARGS,
    FROM_ARGS,
)


if TYPE_CHECKING:
    from dl_formula.core.datatype import DataType
    from dl_formula_ref.registry.arg_base import FuncArg
    import dl_formula_ref.registry.base as _registry_base
    from dl_formula_ref.registry.env import GenerationEnvironment


class TypeInfo(NamedTuple):
    ret_type_str: ParameterizedText
    arg_note: Optional[ParameterizedText]


def type_macro(*types: DataType) -> str:
    return f'{{type: {"|".join(t.name for t in types)}}}'


class TypeStrategyInspector:
    @classmethod
    def _extract_common_type_args(
        cls,
        ret_type_strat: TypeStrategy,
        args: List[FuncArg],
    ) -> List[FuncArg]:
        ct_args = []
        if isinstance(ret_type_strat, FromArgs):
            # in this case the return type is determined from types of arguments
            use_arg_indices: Sequence[Union[int, slice]]
            if isinstance(ret_type_strat, DynamicIndexStrategy):
                use_arg_indices = ret_type_strat.get_indices(len(args))
            else:
                use_arg_indices = ret_type_strat._indices
            for ind in use_arg_indices:
                ct_args.extend(args[ind] if isinstance(ind, slice) else [args[ind]])

        return ct_args

    @classmethod
    def _extract_common_type_args_str(
        cls,
        ret_type_strat: TypeStrategy,
        inf_args: bool,
        args: List[FuncArg],
    ) -> str:
        ct_args = cls._extract_common_type_args(ret_type_strat, args)
        arg_str = ", ".join("`{}`".format(a.name) for a in ct_args)
        if inf_args is None:
            arg_str += ", ..."
        return arg_str

    @classmethod
    def get_return_type_and_arg_type_note(
        cls,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> TypeInfo:
        """
        Return string representing the return type
        and, possibly, a note regarding the argument types
        """
        def_list = item.get_implementation_specs(env=env)
        args = item.get_args(env=env)

        strategies = list({id(defn.return_type): defn.return_type for defn in def_list}.values())
        inf_args = any(defn.arg_cnt is None for defn in def_list)
        note = None
        ret_type_str: ParameterizedText = ParameterizedText(text="")

        # deduplicate const and non-const types
        fixed_types = set()
        ind = 0
        while ind < len(strategies):
            ret_type_strat = strategies[ind]
            if isinstance(ret_type_strat, Fixed):
                if ret_type_strat._type.non_const_type not in fixed_types:
                    fixed_types.add(ret_type_strat._type.non_const_type)
                else:
                    del strategies[ind]
                    continue
            ind += 1

        if len(strategies) == 1:
            ret_type_strat = strategies[0]
            if isinstance(ret_type_strat, Fixed):
                ret_type_str = ParameterizedText(text=type_macro(ret_type_strat.type.non_const_type))
            elif isinstance(ret_type_strat, FromArgs):
                common_type_args_str = cls._extract_common_type_args_str(ret_type_strat, inf_args, args)
                ret_type_str = ParameterizedText(text=FROM_ARGS, params=dict(args=common_type_args_str))
                if "," in common_type_args_str:  # FromArgs strategy is used for more than 1 argument
                    # a note about args that they must have the same type
                    note = ParameterizedText(text=COMMON_TYPE_NOTE, params=dict(args=common_type_args_str))

        else:
            ret_type_str = ParameterizedText(text=DEPENDS_ON_ARGS)

        return TypeInfo(ret_type_str=ret_type_str, arg_note=note)
