from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
)


if TYPE_CHECKING:
    from dl_formula.core.dialect import DialectCombo
    import dl_formula_ref.registry.base as _registry_base
    from dl_formula_ref.registry.env import GenerationEnvironment


class DialectExtractorBase(abc.ABC):
    @abc.abstractmethod
    def get_dialects(
        self,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> set[DialectCombo]:
        raise NotImplementedError
