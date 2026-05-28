"""
Extended formula nodes
"""

from __future__ import annotations

from typing import (
    Hashable,
)

from sqlalchemy.sql import ClauseElement

from dl_formula.core import nodes
from dl_formula.core.extract import NodeExtract
from dl_formula.translation.context import TranslationCtx


class CompiledExpression(nodes.BaseLiteral):
    """Represents a pre-compiled expression that should be substituted as-is"""

    @classmethod
    def make(
        cls,
        value: ClauseElement | TranslationCtx,
        *,
        meta: nodes.NodeMeta | None = None,
    ) -> CompiledExpression:
        return super().make(value=value, meta=meta)

    @classmethod
    def validate_literal_value(cls, literal_value: Hashable | None) -> None:
        assert isinstance(literal_value, (ClauseElement, TranslationCtx))

    def _make_extract(self) -> NodeExtract | None:
        if isinstance(self.value, TranslationCtx):
            return self.value.extract

        return None
