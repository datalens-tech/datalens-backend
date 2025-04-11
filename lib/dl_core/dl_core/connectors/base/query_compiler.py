from __future__ import annotations

from enum import (
    Enum,
    unique,
)
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
    Sequence,
)

import attr
import sqlalchemy as sa
from sqlalchemy.sql.expression import (
    nullsfirst,
    nullslast,
)

from dl_constants.enums import OrderDirection
from dl_core import exc
from dl_core.query.expression import ExpressionCtx


if TYPE_CHECKING:
    from sqlalchemy.engine.default import DefaultDialect
    from sqlalchemy.sql.elements import ClauseElement
    from sqlalchemy.sql.selectable import Select

    from dl_core.query.bi_query import (
        BIQuery,
        SqlSourceType,
    )
    from dl_core.query.expression import OrderByExpressionCtx


@unique
class SectionAliasMode(Enum):
    """
    Compiler configuration that determines how the expressions should be placed
    in e.g. a "group by" section.

    * `unaliased`: e.g. `select {expr} … group by {expr}`
    * `by_alias_in_section`: `select {expr} as col … group by col` (ClickHouse, PostgreSQL, MySQL)
    * `by_alias_in_select`: `select col … group by {expr} as col` (YQL)

    In a more general case, this would be 'section priority order for placing
    the column expression with alias', but there's only 3 known cases anyway.
    """

    aliased = "aliased"
    unaliased = "unaliased"
    by_alias_in_section = "by_alias_in_section"
    by_alias_in_select = "by_alias_in_select"


@attr.s(frozen=True)
class QueryCompiler:
    dialect: DefaultDialect = attr.ib()

    orderby_alias_mode: ClassVar[SectionAliasMode] = SectionAliasMode.by_alias_in_section
    groupby_alias_mode: ClassVar[SectionAliasMode] = SectionAliasMode.unaliased

    force_nulls_lower_than_values: ClassVar[bool] = False

    def quote(self, name: str) -> str:  # TODO: one str is `identifier`, another str is `SQL`
        return self.dialect.identifier_preparer.quote(name)

    def aliased_column(self, expr_ctx: ExpressionCtx) -> ClauseElement:
        alias = expr_ctx.alias
        assert alias, "cannot refer to unaliased expr_ctx with an alias"
        return sa.literal_column(self.quote(alias))

    def make_select_expression(self, expr_ctx: ExpressionCtx, bi_query: BIQuery) -> ClauseElement:
        """Make expression for the SELECT clause"""
        expr = expr_ctx.expression
        if not expr_ctx.alias:
            return expr
        expr_with_alias = expr.label(expr_ctx.alias)  # type: ignore  # 2024-01-24 # TODO: "ClauseElement" has no attribute "label"  [attr-defined]
        # TODO: put this separation into subclasses (if the logic proves to be correct).
        if self.groupby_alias_mode == SectionAliasMode.unaliased:
            return expr_with_alias
        if self.groupby_alias_mode == SectionAliasMode.by_alias_in_section:
            return expr_with_alias
        if self.groupby_alias_mode == SectionAliasMode.by_alias_in_select:
            is_in_group_by = any(sel_expr_ctx.alias == expr_ctx.alias for sel_expr_ctx in bi_query.group_by_expressions)
            if is_in_group_by:
                return self.aliased_column(expr_ctx)
            return expr_with_alias
        raise Exception(f"Unexpected {self.groupby_alias_mode=!r}")

    def make_group_by_expression(self, expr_ctx: ExpressionCtx, bi_query: BIQuery) -> ClauseElement:
        """Make expression for the GROUP BY clause"""
        expr = expr_ctx.expression
        if not expr_ctx.alias:
            return expr
        expr_with_alias = expr.label(expr_ctx.alias)  # type: ignore  # 2024-01-24 # TODO: "ClauseElement" has no attribute "label"  [attr-defined]
        # TODO: put this separation into subclasses (if the logic proves to be correct).
        if self.groupby_alias_mode == SectionAliasMode.unaliased:
            return expr
        if self.groupby_alias_mode == SectionAliasMode.by_alias_in_section:
            is_selected = any(sel_expr_ctx.alias == expr_ctx.alias for sel_expr_ctx in bi_query.select_expressions)
            if is_selected:
                return self.aliased_column(expr_ctx)
            return expr
        if self.groupby_alias_mode == SectionAliasMode.by_alias_in_select:
            return expr_with_alias
        raise Exception(f"Unexpected {self.groupby_alias_mode=!r}")

    def should_order_by_alias(self, expr_ctx: ExpressionCtx, bi_query: BIQuery) -> bool:
        if not expr_ctx.alias:
            return False
        if self.orderby_alias_mode == SectionAliasMode.unaliased:
            return False
        if self.orderby_alias_mode == SectionAliasMode.by_alias_in_section:
            prior_sections = (
                (bi_query.select_expressions, bi_query.group_by_expressions)
                if self.groupby_alias_mode == SectionAliasMode.by_alias_in_select
                else (bi_query.select_expressions,)
            )
            return any(
                prior_expr_ctx.alias == expr_ctx.alias for section in prior_sections for prior_expr_ctx in section
            )
        raise Exception(f"Unexpected {self.orderby_alias_mode=!r}")

    def make_order_by_expression(self, order_by_ctx: OrderByExpressionCtx, bi_query: BIQuery) -> ClauseElement:
        """Make expression for the ORDER BY clause"""
        expr: ClauseElement
        if self.should_order_by_alias(expr_ctx=order_by_ctx, bi_query=bi_query):
            expr = self.aliased_column(order_by_ctx)
        else:
            expr = order_by_ctx.expression

        dir_wrapper = {
            OrderDirection.asc: sa.asc,
            OrderDirection.desc: sa.desc,
        }[order_by_ctx.direction]
        expr = dir_wrapper(expr)

        if self.force_nulls_lower_than_values:
            nulls_wrapper = {
                OrderDirection.asc: nullsfirst,
                OrderDirection.desc: nullslast,
            }[order_by_ctx.direction]
            expr = nulls_wrapper(expr)

        return expr

    def compile_select(self, bi_query: BIQuery, sql_source: Optional[SqlSourceType]) -> Select:
        """Compile a ``BIQuery`` object into an SQLAlchemy ``Select``"""

        def _unwrap_expressions(ex_list: Sequence) -> list[ClauseElement]:
            """Unwrap ``ExpressionCtx`` objects to nested SA expressions"""
            return [ex.expression if isinstance(ex, ExpressionCtx) else ex for ex in ex_list]

        query = sa.select(
            [self.make_select_expression(expr_ctx, bi_query=bi_query) for expr_ctx in bi_query.select_expressions]
        )

        if sql_source is not None:  # support queries without `from`; mostly for gsheets
            query = query.select_from(sql_source)

        for expr in _unwrap_expressions(bi_query.dimension_filters):
            query = query.where(expr)

        for expr in _unwrap_expressions(bi_query.measure_filters):
            query = query.having(expr)

        if bi_query.distinct:
            query = query.distinct()

        query = query.group_by(
            *[self.make_group_by_expression(expr_ctx, bi_query=bi_query) for expr_ctx in bi_query.group_by_expressions]
        )

        for order_by_ctx in bi_query.order_by_expressions:
            query = query.order_by(self.make_order_by_expression(order_by_ctx, bi_query=bi_query))

        if bi_query.limit is not None:
            query = query.limit(bi_query.limit)
        if bi_query.offset is not None:
            if not bi_query.order_by_expressions:
                raise exc.QueryConstructorError("Offset cannot be used without sorting")
            query = query.offset(bi_query.offset)

        return query
