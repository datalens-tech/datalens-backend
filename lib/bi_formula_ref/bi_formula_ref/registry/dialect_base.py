from __future__ import annotations

import abc
from typing import Set, TYPE_CHECKING

if TYPE_CHECKING:
    from bi_formula.core.dialect import DialectCombo
    from bi_formula_ref.registry.env import GenerationEnvironment
    import bi_formula_ref.registry.base as _registry_base


class DialectExtractorBase(abc.ABC):
    @abc.abstractmethod
    def get_dialects(
            self, item: _registry_base.FunctionDocRegistryItem, env: GenerationEnvironment,
    ) -> Set[DialectCombo]:
        raise NotImplementedError
