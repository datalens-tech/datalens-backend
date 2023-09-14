# FIXME: Remove after transition to US-saved field depoendencies

from __future__ import annotations

from typing import Dict

import attr

from bi_core.components.dependencies.factory import ComponentDependencyManagerFactory
from bi_core.components.dependencies.relation_avatar_legacy import LegacyRelationAvatarDependencyManager
from bi_core.components.ids import RelationId
from bi_core.query.expression import ExpressionCtx


@attr.s
class LegacyComponentDependencyManagerFactory(ComponentDependencyManagerFactory):
    _relation_expressions: Dict[RelationId, ExpressionCtx] = attr.ib(factory=list)

    def set_relation_expressions(self, _relation_expressions: Dict[RelationId, ExpressionCtx]) -> None:
        self._relation_expressions = _relation_expressions

    def _make_relation_avatar_dependency_manager(self) -> LegacyRelationAvatarDependencyManager:  # type: ignore  # TODO: fix
        return LegacyRelationAvatarDependencyManager(
            relation_expressions=self._relation_expressions,
        )
