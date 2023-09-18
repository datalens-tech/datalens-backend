from __future__ import annotations

from typing import (
    AbstractSet,
    Set,
)

import attr

from dl_core.components.dependencies.field_deep_base import FieldDeepInterDependencyManagerBase
from dl_core.components.dependencies.field_shallow_base import FieldShallowInterDependencyManagerBase
from dl_core.components.ids import FieldId


@attr.s
class FieldDeepInterDependencyManager(FieldDeepInterDependencyManagerBase):
    _shallow: FieldShallowInterDependencyManagerBase = attr.ib(kw_only=True)

    def get_field_deep_references(self, dep_field_id: FieldId) -> AbstractSet[FieldId]:
        refs: Set[FieldId] = set()

        def _populate_refs_recursively(field_id: FieldId) -> AbstractSet[FieldId]:  # type: ignore  # TODO: fix
            if field_id in refs:
                return set()
            refs.add(field_id)
            new_refs = self._shallow.get_field_direct_references(dep_field_id=field_id)
            for _id in new_refs:
                _populate_refs_recursively(_id)

        _populate_refs_recursively(dep_field_id)
        refs.remove(dep_field_id)
        return refs
