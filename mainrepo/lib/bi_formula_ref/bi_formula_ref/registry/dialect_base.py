from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Set,
)

if TYPE_CHECKING:
    from bi_formula.core.dialect import DialectCombo
    import bi_formula_ref.registry.base as _registry_base
    from bi_formula_ref.registry.env import GenerationEnvironment


class DialectExtractorBase(abc.ABC):
    @abc.abstractmethod
    def get_dialects(
        self,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> Set[DialectCombo]:
        raise NotImplementedError
