from __future__ import annotations

from itertools import chain
from typing import List, Optional, Sequence, Set, Union

from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.selectable import FromClause, Select

import attr

from bi_constants.enums import BIType

from bi_core.query.expression import (
    ExpressionCtx, OrderByExpressionCtx,
)


SqlSourceType = Union[FromClause, TextClause]


@attr.s(frozen=True, auto_attribs=True)
class BIQuery:
    select_expressions: Sequence[ExpressionCtx]
    group_by_expressions: Sequence[ExpressionCtx] = attr.ib(factory=tuple)
    order_by_expressions: Sequence[OrderByExpressionCtx] = attr.ib(factory=tuple)
    dimension_filters: Sequence[ExpressionCtx] = attr.ib(factory=tuple)
    measure_filters: Sequence[ExpressionCtx] = attr.ib(factory=tuple)
    distinct: bool = False
    limit: Optional[int] = None
    offset: Optional[int] = None

    def get_required_avatar_ids(self) -> Set[str]:
        """Collect source avatar references from all expressions in the query."""

        return set(chain.from_iterable(
            [ctx.avatar_ids
             for expr_list in (
                 self.select_expressions,
                 self.group_by_expressions,
                 self.order_by_expressions,
                 self.dimension_filters,
                 self.measure_filters,
             ) if expr_list
             for ctx in expr_list if isinstance(ctx, ExpressionCtx) and ctx.avatar_ids]
        ))


@attr.s
class QueryAndResultInfo:
    query: Select = attr.ib(kw_only=True)
    user_types: List[BIType] = attr.ib(kw_only=True)
    col_names: List[str] = attr.ib(kw_only=True)
