from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import exc
from sqlalchemy.sql import elements
from sqlalchemy.sql.compiler import COLLECT_CARTESIAN_PRODUCTS, WARN_LINTING, FromLinter

from clickhouse_sqlalchemy.drivers.http.base import ClickHouseDialect_http as UPSTREAM

from bi_sqlalchemy_common.base import CompilerPrettyMixin


class BIClickHouseTypeCompiler(UPSTREAM.type_compiler):
    """ ... """

    # # Not currently viable.
    # def visit_datetime(self, type_, **kw):
    #     # for casts? TODO: bi_sqlalchemy_clickhouse_tests.
    #     return "DateTime('UTC')"


class BIClickHouseCompilerBasic(UPSTREAM.statement_compiler):
    """ ... """


class BIClickHouseCompiler(BIClickHouseCompilerBasic, UPSTREAM.statement_compiler, CompilerPrettyMixin):
    """
    A bunch of copypaste from the `ClickHouseCompiler` but building query text
    through `self._pretty`
    """
    def visit_join(self, join, asfrom=False, **kwargs):
        # need to make a variable to prevent leaks in some debuggers
        join_type = getattr(join, 'type', None)
        if join_type is None:
            if join.isouter:
                join_type = 'LEFT OUTER'
            else:
                join_type = 'INNER'
        elif join_type is not None:
            join_type = join_type.upper()
            if join.isouter and 'INNER' in join_type:
                raise exc.CompileError(
                    "can't compile join with specified "
                    "INNER type and isouter=True"
                )
            # isouter=False by default, disable that checking
            # elif not join.isouter and 'OUTER' in join.type:
            #     raise exc.CompileError(
            #         "can't compile join with specified "
            #         "OUTER type and isouter=False"
            #     )
        if join.full and 'FULL' not in join_type:
            join_type = 'FULL ' + join_type

        if getattr(join, 'strictness', None):
            join_type = join.strictness.upper() + ' ' + join_type

        if getattr(join, 'distribution', None):
            join_type = join.distribution.upper() + ' ' + join_type

        onclause = join.onclause
        return self._pretty.join_sql_join(
            left=join.left._compiler_dispatch(self, asfrom=asfrom, **kwargs),
            join_type=(join_type + ' JOIN').upper() if join_type else '',
            right=join.right._compiler_dispatch(self, asfrom=asfrom, **kwargs),
            onclause=onclause._compiler_dispatch(self, **kwargs),
            ontext='USING' if isinstance(onclause, elements.Tuple) else 'ON',
        )

    def _compose_select_body(
        self,
        text,
        select,
        compile_state,
        inner_columns,
        froms,
        byfrom,
        toplevel,
        kwargs,
    ):
        text_pieces = [self._pretty.join_block(text.rstrip(), self._pretty.join_group(inner_columns))]
        del text  # should not be used any further

        # TODO: confirm that we need this part of code in overridden method
        if self.linting & COLLECT_CARTESIAN_PRODUCTS:
            from_linter = FromLinter({}, set())
            warn_linting = self.linting & WARN_LINTING
            if toplevel:
                self.from_linter = from_linter
        else:
            from_linter = None  # type: ignore
            warn_linting = False

        if froms:
            if select._hints:
                from_sql = self._pretty.join_group(
                    [
                        f._compiler_dispatch(
                            self,
                            asfrom=True,
                            fromhints=byfrom,
                            from_linter=from_linter,
                            **kwargs
                        )
                        for f in froms
                    ]
                )
            else:
                from_sql = self._pretty.join_group(
                    [
                        f._compiler_dispatch(
                            self,
                            asfrom=True,
                            from_linter=from_linter,
                            **kwargs
                        )
                        for f in froms
                    ]
                )
            text_pieces.append(self._pretty.join_block("FROM", from_sql, block_indent=False))
        else:
            text_pieces.append(self.default_from())

        if getattr(select, '_array_join', None) is not None:
            text_pieces.append(select._array_join._compiler_dispatch(self, **kwargs))

        sample_clause = getattr(select, '_sample_clause', None)

        if sample_clause is not None:
            text_pieces.append(self.sample_clause(select, **kwargs))

        final_clause = getattr(select, '_final_clause', None)

        if final_clause is not None:
            text_pieces.append(self.final_clause())

        if select._where_criteria:
            t = self._generate_delimited_and_list(
                select._where_criteria, from_linter=from_linter, **kwargs
            )
            if t:
                text_pieces.append(self._pretty.join_block("WHERE", t))

        if warn_linting:
            from_linter.warn()

        if select._group_by_clauses:
            text_pieces.append(self.group_by_clause(select, **kwargs))

        if select._having_criteria:
            t = self._generate_delimited_and_list(select._having_criteria, **kwargs)
            if t:
                text_pieces.append(self._pretty.join_block("HAVING", t))

        if select._order_by_clauses:
            t = self.order_by_clause(select, **kwargs)
            text_pieces.append(self._pretty.indent(t))

        if select._has_row_limiting_clause:
            text_pieces.append(self._row_limit_clause(select, **kwargs))

        if select._for_update_arg is not None:
            text_pieces.append(self.for_update_clause(select, **kwargs))

        return self._pretty.join(text_pieces)

    # Copypaste of `CompilerPrettyMixin`, just to confirm that the same
    # processing is okay for the overridden methods.
    def group_by_clause(self, select, **kw):
        sup = super().group_by_clause(select, **kw)
        return self._pretty.postprocess_block("GROUP BY", sup)

    def limit_clause(self, select, **kw):
        sup = super().limit_clause(select, **kw)
        return self._pretty.postprocess_block("LIMIT", sup)


class BIClickHouseDialectBasic(UPSTREAM):

    type_compiler = BIClickHouseTypeCompiler
    statement_compiler = BIClickHouseCompilerBasic


class BIClickHouseDialect(BIClickHouseDialectBasic):
    statement_compiler = BIClickHouseCompiler


def register_dialect():
    sa.dialects.registry.register("bi_clickhouse_basic", "bi_sqlalchemy_clickhouse.base", "BIClickHouseDialectBasic")
    sa.dialects.registry.register("bi_clickhouse", "bi_sqlalchemy_clickhouse.base", "BIClickHouseDialect")
