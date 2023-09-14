"""
Extended formula nodes
"""

from __future__ import annotations

from typing import (
    Hashable,
    Optional,
)

from sqlalchemy.sql import ClauseElement

from bi_formula.core import nodes
from bi_formula.core.extract import NodeExtract
from bi_formula.translation.context import TranslationCtx


class CompiledExpression(nodes.BaseLiteral):
    """Represents a pre-compiled expression that should be substituted as-is"""

    @classmethod
    def make(
        cls,
        value: ClauseElement | TranslationCtx,
        *,
        meta: Optional[nodes.NodeMeta] = None,
    ) -> CompiledExpression:
        return super().make(value=value, meta=meta)

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        assert isinstance(literal_value, (ClauseElement, TranslationCtx))

    def _make_extract(self) -> Optional[NodeExtract]:
        if isinstance(self.value, TranslationCtx):
            return self.value.extract

        return None
