from __future__ import annotations

from typing import (
    AbstractSet,
    Collection,
    List,
    Optional,
)

import attr

from bi_core.components.dependencies.field_shallow_base import FieldShallowInterDependencyManagerBase
from bi_core.components.dependencies.primitives import (
    FieldInterDependencyInfo,
    FieldInterDependencyItem,
)
from bi_core.components.ids import FieldId


@attr.s
class FieldShallowInterDependencyManager(FieldShallowInterDependencyManagerBase):
    _inter_dep_info: FieldInterDependencyInfo = attr.ib(kw_only=FieldInterDependencyInfo)  # type: ignore  # TODO: fix

    @property
    def _direct_dependencies(self) -> List[FieldInterDependencyItem]:
        return self._inter_dep_info.deps

    def _get_item_for_field(self, dep_field_id: FieldId) -> Optional[FieldInterDependencyItem]:  # type: ignore  # TODO: fix
        for item in self._direct_dependencies:
            if item.dep_field_id == dep_field_id:
                return item

    def set_field_direct_references(self, dep_field_id: FieldId, ref_field_ids: Collection[FieldId]) -> None:
        item = self._get_item_for_field(dep_field_id=dep_field_id)
        if item is None:
            item = FieldInterDependencyItem(dep_field_id=dep_field_id)
            self._direct_dependencies.append(item)

        assert item is not None
        item.ref_field_ids = set(ref_field_ids)

        if not item.ref_field_ids:
            self._direct_dependencies.remove(item)

    def clear_field_direct_references(self, dep_field_id: FieldId) -> None:
        item = self._get_item_for_field(dep_field_id=dep_field_id)
        if item is not None:
            self._direct_dependencies.remove(item)

    def get_field_direct_references(self, dep_field_id: FieldId) -> AbstractSet[FieldId]:
        item = self._get_item_for_field(dep_field_id=dep_field_id)
        if item is not None:
            ref_field_ids = item.ref_field_ids
        else:
            ref_field_ids = set()
        return ref_field_ids
