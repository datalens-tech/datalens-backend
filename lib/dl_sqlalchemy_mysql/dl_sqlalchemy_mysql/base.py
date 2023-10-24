from __future__ import annotations

import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql as UPSTREAM

from dl_sqlalchemy_common.base import CompilerPrettyMixin


class DLMYSQLCompilerBasic(UPSTREAM.statement_compiler):
    """Necessary overrides"""

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


class DLMYSQLDialect(DLMYSQLDialectBasic):
    statement_compiler = DLMYSQLCompiler


def register_dialect():
    sa.dialects.registry.register("dl_mysql_basic", "dl_sqlalchemy_mysql.base", "DLMYSQLDialectBasic")
    sa.dialects.registry.register("dl_mysql", "dl_sqlalchemy_mysql.base", "DLMYSQLDialect")
