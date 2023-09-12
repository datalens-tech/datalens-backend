from __future__ import annotations

from bi_core.query.expression import ExpressionCtx
from bi_core.query.bi_query import BIQuery
from bi_core.connectors.base.query_compiler import QueryCompiler


class MSSQLQueryCompiler(QueryCompiler):
    def should_order_by_alias(self, expr_ctx: ExpressionCtx, bi_query: BIQuery) -> bool:
        # No idea, why MSSQL acts this way, but it just does
        if bi_query.limit is not None and bi_query.offset is not None:
            return False
        return super().should_order_by_alias(expr_ctx=expr_ctx, bi_query=bi_query)
