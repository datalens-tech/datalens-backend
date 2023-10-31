from __future__ import annotations

from itertools import chain
from typing import (
    Optional,
    Sequence,
    Union,
)

import attr
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.selectable import (
    FromClause,
    Select,
)

from dl_constants.enums import UserDataType
from dl_core.query.expression import (
    ExpressionCtx,
    OrderByExpressionCtx,
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

    def get_names(self) -> list[str]:
        names: list[str] = []
        for expr_ctx in self.select_expressions:
            assert expr_ctx.alias is not None
            names.append(expr_ctx.alias)
        return names

    def get_user_types(self) -> list[UserDataType]:
        user_types: list[UserDataType] = []
        for expr_ctx in self.select_expressions:
            assert expr_ctx.user_type is not None
            user_types.append(expr_ctx.user_type)
        return user_types

    def get_required_avatar_ids(self) -> set[str]:
        """Collect source avatar references from all expressions in the query."""

        return set(
            chain.from_iterable(
                [
                    ctx.avatar_ids
                    for expr_list in (
                        self.select_expressions,
                        self.group_by_expressions,
                        self.order_by_expressions,
                        self.dimension_filters,
                        self.measure_filters,
                    )
                    if expr_list
                    for ctx in expr_list
                    if isinstance(ctx, ExpressionCtx) and ctx.avatar_ids
                ]
            )
        )


@attr.s
class QueryAndResultInfo:
    query: Select = attr.ib(kw_only=True)
    user_types: list[UserDataType] = attr.ib(kw_only=True)
    col_names: list[str] = attr.ib(kw_only=True)
