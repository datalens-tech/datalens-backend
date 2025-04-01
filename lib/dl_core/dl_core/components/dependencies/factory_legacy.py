# FIXME: Remove after transition to US-saved field depoendencies

from __future__ import annotations

import attr

from dl_core.components.dependencies.factory import ComponentDependencyManagerFactory
from dl_core.components.dependencies.relation_avatar_legacy import LegacyRelationAvatarDependencyManager
from dl_core.components.ids import RelationId
from dl_core.query.expression import ExpressionCtx


@attr.s
class LegacyComponentDependencyManagerFactory(ComponentDependencyManagerFactory):
    _relation_expressions: dict[RelationId, ExpressionCtx] = attr.ib(factory=list)

    def set_relation_expressions(self, _relation_expressions: dict[RelationId, ExpressionCtx]) -> None:
        self._relation_expressions = _relation_expressions

    def _make_relation_avatar_dependency_manager(self) -> LegacyRelationAvatarDependencyManager:  # type: ignore  # TODO: fix
        return LegacyRelationAvatarDependencyManager(
            relation_expressions=self._relation_expressions,
        )
