from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    List,
    NamedTuple,
    Set,
)

from dl_formula.core.datatype import DataType


if TYPE_CHECKING:
    import dl_formula_ref.registry.base as _registry_base
    from dl_formula_ref.registry.env import GenerationEnvironment


class FuncArg(NamedTuple):
    types: Set[DataType]
    name: str
    optional_level: int

    @property
    def formal_type(self) -> str:
        return "|".join(sorted([t.name for t in self.types])) or "ANY"

    @property
    def is_const(self) -> bool:
        return any(t.is_const for t in self.types)


class ArgumentExtractorBase(abc.ABC):
    @abc.abstractmethod
    def get_args(self, item: _registry_base.FunctionDocRegistryItem, env: GenerationEnvironment) -> List[FuncArg]:
        raise NotImplementedError
