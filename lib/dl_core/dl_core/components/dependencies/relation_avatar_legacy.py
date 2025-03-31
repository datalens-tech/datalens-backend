# FIXME: remove this after full transition to saved field dependencies in favor of RelationAvatarDependencyManager

from __future__ import annotations

from typing import (
    AbstractSet,
)

import attr

from dl_core.components.dependencies.relation_avatar_base import RelationAvatarDependencyManagerBase
from dl_core.components.ids import (
    AvatarId,
    RelationId,
)
from dl_core.query.expression import ExpressionCtx


@attr.s
class LegacyRelationAvatarDependencyManager(RelationAvatarDependencyManagerBase):
    _relation_expressions: dict[RelationId, ExpressionCtx] = attr.ib(kw_only=True)

    def get_relation_avatar_references(self, relation_id: RelationId) -> AbstractSet[AvatarId]:
        """
        Return a set of all avatars referenced directly or indirectly by the given relation.
        """
        return frozenset(self._relation_expressions[relation_id].avatar_ids)  # type: ignore  # TODO: fix
