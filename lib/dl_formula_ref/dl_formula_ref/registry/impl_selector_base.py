from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
)


if TYPE_CHECKING:
    from dl_formula_ref.registry.base import FunctionDocRegistryItem
    from dl_formula_ref.registry.env import GenerationEnvironment
    from dl_formula_ref.registry.impl_spec import FunctionImplementationSpec


class ImplementationSelectorBase(abc.ABC):
    @abc.abstractmethod
    def get_implementations(
        self,
        item: FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> list[FunctionImplementationSpec]:
        raise NotImplementedError
