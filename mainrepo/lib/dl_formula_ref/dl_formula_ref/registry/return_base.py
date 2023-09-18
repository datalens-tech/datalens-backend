from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import dl_formula_ref.registry.base as _registry_base
    from dl_formula_ref.registry.env import GenerationEnvironment
    from dl_formula_ref.registry.text import ParameterizedText


class ReturnTypeExtractorBase(abc.ABC):
    @abc.abstractmethod
    def get_return_type(
        self,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> ParameterizedText:
        raise NotImplementedError
