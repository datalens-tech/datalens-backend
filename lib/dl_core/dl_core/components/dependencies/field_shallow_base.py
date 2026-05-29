from __future__ import annotations

import abc
from collections.abc import (
    Collection,
    Set,
)

from dl_core.components.ids import FieldId


class FieldShallowInterDependencyManagerBase(abc.ABC):
    @abc.abstractmethod
    def set_field_direct_references(self, dep_field_id: FieldId, ref_field_ids: Collection[FieldId]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_field_direct_references(self, dep_field_id: FieldId) -> Set[FieldId]:
        raise NotImplementedError
