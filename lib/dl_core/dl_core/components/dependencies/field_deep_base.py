from __future__ import annotations

import abc
from collections.abc import Set

from dl_core.components.ids import FieldId


class FieldDeepInterDependencyManagerBase(abc.ABC):
    """Builds deep field dependencies"""

    @abc.abstractmethod
    def get_field_deep_references(self, dep_field_id: FieldId) -> Set[FieldId]:
        """
        Walk field dependencies and collect directly referenced fields,
        fields referenced by directly referenced fields and so on.
        Return a set of these referenced fields
        """
        raise NotImplementedError
