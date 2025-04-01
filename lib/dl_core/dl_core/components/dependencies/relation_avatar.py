from __future__ import annotations

from typing import AbstractSet

import attr

from dl_core.components.dependencies.field_avatar_base import FieldAvatarDependencyManagerBase
from dl_core.components.dependencies.relation_avatar_base import RelationAvatarDependencyManagerBase
from dl_core.components.ids import (
    AvatarId,
    RelationId,
)
from dl_core.multisource import (
    AvatarRelation,
    BinaryCondition,
    ConditionPart,
    ConditionPartDirect,
    ConditionPartResultField,
)


@attr.s
class RelationAvatarDependencyManager(RelationAvatarDependencyManagerBase):
    _avatar_relations: list[AvatarRelation] = attr.ib(kw_only=True)  # FIXME: Replace with RelationManager or smth?
    _field_avatar_mgr: FieldAvatarDependencyManagerBase = attr.ib(kw_only=True)

    def _get_relation(self, relation_id: RelationId) -> AvatarRelation:
        for relation in self._avatar_relations:
            if relation.id == relation_id:
                return relation
        raise KeyError(relation_id)

    def _get_condition_part_avatar_ids(
        self,
        default_avatar_id: AvatarId,
        part: ConditionPart,
    ) -> AbstractSet[AvatarId]:
        if isinstance(part, ConditionPartDirect):
            return {default_avatar_id}
        elif isinstance(part, ConditionPartResultField):
            return self._field_avatar_mgr.get_field_avatar_references(part.field_id)
        raise TypeError(f"Unsupported type for part: {type(part)}")

    def _get_condition_avatar_ids(
        self,
        relation: AvatarRelation,
        condition: BinaryCondition,
    ) -> AbstractSet[AvatarId]:
        return self._get_condition_part_avatar_ids(
            part=condition.left_part,
            default_avatar_id=relation.left_avatar_id,
        ) | self._get_condition_part_avatar_ids(
            part=condition.right_part,
            default_avatar_id=relation.right_avatar_id,
        )

    def get_relation_avatar_references(self, relation_id: RelationId) -> AbstractSet[AvatarId]:
        relation = self._get_relation(relation_id)
        avatar_refs: set[AvatarId] = set()
        for condition in relation.conditions:
            assert isinstance(condition, BinaryCondition)
            avatar_refs |= self._get_condition_avatar_ids(relation=relation, condition=condition)

        return avatar_refs
