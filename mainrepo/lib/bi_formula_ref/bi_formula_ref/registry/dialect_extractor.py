from __future__ import annotations

from typing import TYPE_CHECKING

from bi_formula.core.dialect import DialectCombo
from bi_formula.core.dialect import StandardDialect as D
from bi_formula_ref.registry.dialect_base import DialectExtractorBase

if TYPE_CHECKING:
    import bi_formula_ref.registry.base as _registry_base
    from bi_formula_ref.registry.env import GenerationEnvironment


# Lowest versions of all backends that are compatible with COMPENG. Filled from plugins
COMPENG_SUPPORT: set[DialectCombo] = set()
EXPAND_COMPENG = True


def register_compeng_support_dialects(any_dialects: frozenset[DialectCombo]) -> None:
    COMPENG_SUPPORT.update(any_dialects)


class DefaultDialectExtractor(DialectExtractorBase):
    def get_dialects(
        self,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> set[DialectCombo]:
        if item.category.name == "window":  # TODO: Maybe separate into two different classes?
            if EXPAND_COMPENG:
                return set(dialect for dialect in COMPENG_SUPPORT if dialect in env.supported_dialects)
            return set()

        def_list = item.get_implementation_specs(env=env)
        dialect_mask = D.EMPTY
        for defn in def_list:
            dialect_mask |= defn.dialects

        return {dialect for dialect in dialect_mask.to_list() if dialect in env.supported_dialects}
