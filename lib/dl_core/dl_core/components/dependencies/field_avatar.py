from __future__ import annotations

from typing import (
    AbstractSet,
    Set,
)

import attr

from dl_constants.enums import CalcMode
from dl_core.components.dependencies.field_avatar_base import FieldAvatarDependencyManagerBase
from dl_core.components.dependencies.field_deep_base import FieldDeepInterDependencyManagerBase
from dl_core.components.ids import (
    AvatarId,
    FieldId,
)
from dl_core.fields import ResultSchema


@attr.s
class FieldAvatarDependencyManager(FieldAvatarDependencyManagerBase):
    _result_schema: ResultSchema = attr.ib(kw_only=True)
    _deep: FieldDeepInterDependencyManagerBase = attr.ib(kw_only=True)

    def get_field_avatar_references(self, dep_field_id: FieldId) -> AbstractSet[AvatarId]:
        deep_refs = self._deep.get_field_deep_references(dep_field_id=dep_field_id) | {dep_field_id}

        avatar_refs: Set[AvatarId] = set()
        for ref_field_if in deep_refs:
            field = self._result_schema.by_guid(ref_field_if)
            if field.calc_mode == CalcMode.direct:
                assert field.avatar_id is not None
                avatar_refs.add(field.avatar_id)

        return avatar_refs
