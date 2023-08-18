from __future__ import annotations

from collections import defaultdict
from functools import lru_cache
from typing import TYPE_CHECKING

from bi_formula.core.dialect import StandardDialect as D
from bi_formula.core.datatype import DataType
from bi_formula.definitions.flags import ContextFlag
from bi_formula.definitions.registry import OPERATION_REGISTRY
from bi_formula.definitions.base import MultiVariantTranslation

from bi_formula_ref.registry.arg_extractor import INFINITE_ARG_COUNT
from bi_formula_ref.registry.impl_selector_base import ImplementationSelectorBase
from bi_formula_ref.registry.impl_spec import FunctionImplementationSpec


if TYPE_CHECKING:
    from bi_formula_ref.registry.env import GenerationEnvironment
    from bi_formula_ref.registry.base import FunctionDocRegistryItem


def _is_deprecated(defn: FunctionImplementationSpec) -> bool:
    return bool(defn.return_flags & ContextFlag.DEPRECATED)


def _make_spec_from_implementation(impl: MultiVariantTranslation) -> FunctionImplementationSpec:
    dialects = D.EMPTY
    for variant in impl.get_variants():
        dialects |= variant.dialects

    assert impl.name is not None
    impl_spec = FunctionImplementationSpec(
        name=impl.name,
        arg_cnt=impl.arg_cnt,
        arg_names=impl.arg_names or (),
        argument_types=impl.argument_types or (),
        return_type=impl.return_type,
        scopes=impl.scopes,
        dialects=dialects,
        return_flags=impl.return_flags,
    )
    return impl_spec


@lru_cache
def _get_implementation_map(env: GenerationEnvironment) -> dict[tuple[str, bool], list[FunctionImplementationSpec]]:
    result: dict[tuple[str, bool], list[FunctionImplementationSpec]] = defaultdict(list)
    for key, impl in OPERATION_REGISTRY.items():
        assert isinstance(impl, MultiVariantTranslation)
        impl_spec = _make_spec_from_implementation(impl)
        if _is_deprecated(impl_spec) or impl_spec.scopes & env.scopes != env.scopes:
            continue
        result[(key.name, key.is_window)].append(impl_spec)
    return result


class EmptyImplementationSelector(ImplementationSelectorBase):
    def get_implementations(
            self, item: FunctionDocRegistryItem, env: GenerationEnvironment,
    ) -> list[FunctionImplementationSpec]:
        return []


class DefaultImplementationSelector(ImplementationSelectorBase):
    def get_implementations(
            self, item: FunctionDocRegistryItem, env: GenerationEnvironment,
    ) -> list[FunctionImplementationSpec]:
        return _get_implementation_map(env=env)[(item.name, item.is_window)]


class ArgAwareImplementationSelector(ImplementationSelectorBase):
    def __init__(self, exp_arg_types: dict[int, set[DataType]]):
        self.exp_arg_types = exp_arg_types

    def get_implementations(
        self, item: FunctionDocRegistryItem, env: GenerationEnvironment
    ) -> list[FunctionImplementationSpec]:
        result: list[FunctionImplementationSpec] = []
        for impl in _get_implementation_map(env=env)[(item.name, item.is_window)]:
            actual_arg_types: dict[int, set[DataType]] = defaultdict(set)
            if impl.argument_types is None:
                continue
            for arg_type_matcher in impl.argument_types:
                for pos in self.exp_arg_types.keys():
                    actual_arg_types[pos] |= arg_type_matcher.get_possible_arg_types_at_pos(
                        pos=pos,
                        total=impl.arg_cnt or INFINITE_ARG_COUNT
                    )
            for pos, arg_types in self.exp_arg_types.items():
                if pos not in actual_arg_types or pos in actual_arg_types and not (actual_arg_types[pos] & arg_types):
                    break
            else:
                result.append(impl)
        return result
