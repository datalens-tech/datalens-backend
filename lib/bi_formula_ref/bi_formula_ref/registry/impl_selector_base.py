from __future__ import annotations

import abc
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from bi_formula_ref.registry.env import GenerationEnvironment
    from bi_formula_ref.registry.base import FunctionDocRegistryItem
    from bi_formula_ref.registry.impl_spec import FunctionImplementationSpec


class ImplementationSelectorBase(abc.ABC):
    @abc.abstractmethod
    def get_implementations(
            self, item: FunctionDocRegistryItem, env: GenerationEnvironment,
    ) -> List[FunctionImplementationSpec]:
        raise NotImplementedError
