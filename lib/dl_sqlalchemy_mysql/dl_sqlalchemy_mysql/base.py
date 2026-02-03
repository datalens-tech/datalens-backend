from __future__ import annotations

import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql as UPSTREAM

from dl_sqlalchemy_common.base import CompilerPrettyMixin


class DLMYSQLCompilerBasic(UPSTREAM.statement_compiler):
    """Necessary overrides"""

    def _add_collate(self, result):
        collate = self.dialect.enforce_collate
        if collate:
            return "%s COLLATE %s" % (result, collate)
        return result

    def visit_like_op_binary(self, binary, operator, **kw):
        result = super().visit_like_op_binary(binary, operator, **kw)
        return self._add_collate(result)

    def visit_not_like_op_binary(self, binary, operator, **kw):
        result = super().visit_not_like_op_binary(binary, operator, **kw)
        return self._add_collate(result)

    def visit_grouping(self, grouping, asfrom=False, add_grouping_collate=False, **kwargs):
        if add_grouping_collate:
            result = grouping.element._compiler_dispatch(self, **kwargs)
            result = self._add_collate(result)
            return "(%s)" % (result,)
        return super().visit_grouping(grouping, asfrom=asfrom, **kwargs)

    def _func_with_collate(self, func, **kwargs):
        result = "%s%s" % (
            func.name,
            self.function_argspec(func, add_grouping_collate=True, **kwargs),
        )
        return result

    def visit_lower_func(self, func, **kwargs):
        return self._func_with_collate(func, **kwargs)

    def visit_upper_func(self, func, **kwargs):
        return self._func_with_collate(func, **kwargs)

    def visit_max_func(self, func, **kwargs):
        return self._func_with_collate(func, **kwargs)

    def visit_min_func(self, func, **kwargs):
        return self._func_with_collate(func, **kwargs)

    def order_by_clause(self, select, **kw):
        order_by = select._order_by_clause
        if order_by is not None and len(order_by):
            order_by_clauses = []
            for element in order_by:
                text = self.process(element, **kw)
                if self.dialect.enforce_collate:
                    text = self._add_collate(text)
                order_by_clauses.append(text)
            return " ORDER BY " + ", ".join(order_by_clauses)
        return ""

    def group_by_clause(self, select, **kw):
        group_by = select._group_by_clause
        if group_by is not None and len(group_by):
            group_by_clauses = []
            for element in group_by:
                text = self.process(element, **kw)
                if self.dialect.enforce_collate:
                    text = self._add_collate(text)
                group_by_clauses.append(text)
            return " GROUP BY " + ", ".join(group_by_clauses)
        return ""

    # allow date and datetime literal_binds
    def render_literal_value(self, value, type_):
        type_to_literal = {
            sa.Date: "DATE",
            sa.DateTime: "TIMESTAMP",
        }

        value_converters = {
            datetime.date: lambda d: d.isoformat(),
            datetime.datetime: lambda dt: dt.replace(tzinfo=None).isoformat(),
            str: lambda s: s.removesuffix("+00:00"),
        }

        for type_key, type_literal in type_to_literal.items():
            if isinstance(type_, type_key):
                for value_type, value_conv in value_converters.items():
                    if isinstance(value, value_type):
                        return "{type_key}'{value_string}'".format(
                            type_key=type_literal,
                            value_string=value_conv(value),
                        )

        return super().render_literal_value(value, type_)


class DLMYSQLCompiler(DLMYSQLCompilerBasic, UPSTREAM.statement_compiler, CompilerPrettyMixin):
    """Added prettification"""

    def limit_clause(self, select, **kw):
        sup = super().limit_clause(select, **kw)
        return self._pretty.postprocess_block("LIMIT", sup)

    def visit_join(self, join, asfrom=False, from_linter=None, **kwargs):
        if from_linter:
            from_linter.edges.add((join.left, join.right))

        if join.full:
            join_type = " FULL OUTER JOIN "
        elif join.isouter:
            join_type = " LEFT OUTER JOIN "
        elif getattr(join, "type", None) == "right":
            join_type = " RIGHT OUTER JOIN "
        else:
            join_type = " INNER JOIN "

        return self._pretty.join_sql_join(
            left=self.process(join.left, asfrom=True, from_linter=from_linter, **kwargs),
            join_type=join_type,
            right=self.process(join.right, asfrom=True, from_linter=from_linter, **kwargs),
            onclause=self.process(join.onclause, from_linter=from_linter, **kwargs),
        )


class DLMYSQLDialectBasic(UPSTREAM):
    statement_compiler = DLMYSQLCompilerBasic

    def __init__(self, enforce_collate=None, **kwargs):
        self.enforce_collate = enforce_collate
        super().__init__(**kwargs)


class DLMYSQLDialect(DLMYSQLDialectBasic):
    statement_compiler = DLMYSQLCompiler


def register_dialect():
    sa.dialects.registry.register("dl_mysql_basic", "dl_sqlalchemy_mysql.base", "DLMYSQLDialectBasic")
    sa.dialects.registry.register("dl_mysql", "dl_sqlalchemy_mysql.base", "DLMYSQLDialect")
