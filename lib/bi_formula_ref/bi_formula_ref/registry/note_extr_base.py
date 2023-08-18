from __future__ import annotations

import abc
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from bi_formula_ref.registry.env import GenerationEnvironment
    import bi_formula_ref.registry.base as _registry_base
    from bi_formula_ref.registry.note import ParameterizedNote


class NoteExtractorBase(abc.ABC):
    @abc.abstractmethod
    def get_notes(
            self, item: _registry_base.FunctionDocRegistryItem, env: GenerationEnvironment,
    ) -> List[ParameterizedNote]:
        raise NotImplementedError
