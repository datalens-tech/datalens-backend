from __future__ import annotations

import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2 as UPSTREAM
from sqlalchemy.sql import sqltypes
from typing import Optional, Any

from bi_sqlalchemy_common.base import CompilerPrettyMixin


class CITEXT(sqltypes.TEXT):

    """Provide the PostgreSQL CITEXT type.

    .. versionadded:: 2.0.7

    """

    __visit_name__ = "CITEXT"

    def coerce_compared_value(
        self, op: Optional[Any], value: Any
    ) -> sqltypes.TypeEngine[Any]:
        return self


class BIPGCompilerBasic(UPSTREAM.statement_compiler):
    """ Necessary overrides """

    def _add_collate(self, result):
        collate = self.dialect.enforce_collate
        if collate:
            return '%s COLLATE %s' % (
                result,
                self.dialect.identifier_preparer.quote(collate))
        return result

    def visit_ilike_op_binary(self, *args, **kwargs):
        result = super().visit_ilike_op_binary(*args, **kwargs)
        return self._add_collate(result)

    def visit_notilike_op_binary(self, *args, **kwargs):
        result = super().visit_notilike_op_binary(*args, **kwargs)
        return self._add_collate(result)

    def visit_grouping(self, grouping, asfrom=False, add_grouping_collate=False, **kwargs):
        if add_grouping_collate:
            result = grouping.element._compiler_dispatch(self, **kwargs)
            result = self._add_collate(result)
            return '(%s)' % (result,)
        return super().visit_grouping(grouping, asfrom=asfrom, **kwargs)

    def _func_with_collate(self, func, **kwargs):
        # See also: sqlalchemy.sql.compiler.SQLCompiler.visit_function
        result = "%s%s" % (
            func.name,
            self.function_argspec(func, add_grouping_collate=True, **kwargs),
        )
        # To consider, instead of `visit_grouping`:
        # assert result.endswith(')')
        # return self._add_collate(result[:-1]) + ')'
        return result

    def visit_lower_func(self, func, **kwargs):
        return self._func_with_collate(func, **kwargs)

    def visit_upper_func(self, func, **kwargs):
        return self._func_with_collate(func, **kwargs)

    # allow datetime literal_binds

    def render_literal_value(self, value, type_):
        # The behavior of literal_binds=True select should match the behavior
        # of psycopg selects.
        if isinstance(type_, sa.Date):
            if isinstance(value, datetime.date):
                return "'{}'::date".format(value.isoformat())

        if isinstance(type_, sa.DateTime):
            if isinstance(value, datetime.datetime):
                # This should've been `type_.timezone` but it didn't go well
                if value.tzinfo is not None:
                    type_name = 'timestamp with time zone'
                    value = value.astimezone(datetime.timezone.utc)
                    value = value.replace(tzinfo=None)
                else:
                    type_name = 'timestamp'
                    if value.tzinfo is not None:
                        value = value.replace(tzinfo=None)
                return "'{}'::{}".format(value.isoformat(), type_name)

        return super().render_literal_value(value, type_)


class BIPGCompiler(BIPGCompilerBasic, UPSTREAM.statement_compiler, CompilerPrettyMixin):
    """ Added prettification """

    def limit_clause(self, select, **kw):
        sup = super().limit_clause(select, **kw)
        return self._pretty.postprocess_block("LIMIT", sup)


class BICustomPGTypeCompiler(UPSTREAM.type_compiler):

    def visit_CITEXT(self, type_, **kw):
        return "CITEXT"


bi_pg_ischema_names = UPSTREAM.ischema_names.copy()
bi_pg_ischema_names.update({'citext': CITEXT})


class BIPGDialectBasic(UPSTREAM):
    """ ... """

    def __init__(self, enforce_collate=None, **kwargs):
        # Side note: due to how sqlalchemy determines dialect arguments,
        # putting `*args` right after `self` breaks the passthrough.
        self.enforce_collate = enforce_collate
        super().__init__(**kwargs)

    type_compiler = BICustomPGTypeCompiler
    statement_compiler = BIPGCompilerBasic
    ischema_names = bi_pg_ischema_names


class BIPGDialect(BIPGDialectBasic):
    statement_compiler = BIPGCompiler


def register_dialect():
    sa.dialects.registry.register("bi_postgresql_basic", "bi_sqlalchemy_postgres.base", "BIPGDialectBasic")
    sa.dialects.registry.register("bi_postgresql", "bi_sqlalchemy_postgres.base", "BIPGDialect")
