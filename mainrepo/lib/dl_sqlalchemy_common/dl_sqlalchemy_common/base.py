from __future__ import annotations

import itertools
from typing import (
    Optional,
    Sequence,
)

import sqlalchemy as sa


class Formatter:
    indenter: str = "  "
    separator: str = "\n"
    inline_separator: str = " "
    preferred_width: int = 120

    def join(self, pieces: Sequence[str], cleanup: bool = True, separator: Optional[str] = None) -> str:
        if separator is None:
            separator = self.separator
        if cleanup:
            pieces = [item.rstrip() for item in pieces]  # type: ignore
            pieces = [item for item in pieces if item]  # type: ignore
        return separator.join(pieces)

    def indent(self, text: str) -> str:
        return self.indenter + text.replace("\n", "\n" + self.indenter)

    def join_ext(self, pieces: Sequence[str], extra_separator: str) -> str:
        """
        Join either into "piece1, piece2" or "piece1,\npiece2"
        (^ example for `extra_separator=","`)
        """
        pieces = list(pieces)
        pieces_len = sum(len(piece) for piece in pieces)
        # Note that this isn't necessarily correct as the further indentation
        # isn't taken into account.
        inline_len = pieces_len + (len(extra_separator) + len(self.inline_separator)) * (
            len(pieces) - 1 if pieces else 0
        )
        if inline_len <= self.preferred_width:
            return self.join(pieces, separator=(extra_separator + self.inline_separator))
        return self.join(pieces, separator=(extra_separator + self.separator))

    def join_group(self, pieces: Sequence[str]) -> str:
        return self.join_ext(pieces, extra_separator=",")

    def join_block(self, name: str, contents: str, block_indent: bool = True) -> str:
        inline_pieces = [name, self.inline_separator, contents]
        if (
            "\n" not in name
            and "\n" not in contents
            and sum(len(item) for item in inline_pieces) <= self.preferred_width
        ):
            return "".join(inline_pieces)
        return self.join((name, self.indent(contents) if block_indent else contents))

    def postprocess_block(self, sql_block_name: str, sql_text: str):
        sql_text_clean = sql_text.strip()
        if sql_text_clean.startswith(sql_block_name):
            sql_text_clean = sql_text_clean.removeprefix(sql_block_name).lstrip()
            return self.join_block(sql_block_name, sql_text_clean)
        return sql_text

    def join_sql_join(
        self,
        left: str,
        join_type: str,
        right: str,
        onclause: str,
        ontext: str = "ON",
    ):
        return self.join_ext(
            (
                left,
                join_type.strip(),
                right,
                self.join_block(ontext.strip(), onclause),
            ),
            extra_separator="",
        )

    def reparenthesize(self, sql_text: str) -> str:
        if not (sql_text.startswith("(") and sql_text.endswith(")")):
            return sql_text
        if "\n" not in sql_text:
            return sql_text
        return self.join(
            (
                "(",
                self.indent(sql_text.removeprefix("(").removesuffix(")")),
                ")",
            )
        )


def _cls_keys(cls: type) -> set[str]:
    return {key for key in cls.__dict__ if not key.startswith("__")}


class CompilerPrettyMixin(sa.sql.compiler.SQLCompiler):  # type: ignore  # TODO: fix
    """
    Compiler with SQL prettification.

    Primarily, adds newlines in some places.

    Additionally might add support for `right join` and other
    common-but-not-omnipresent features.

    The recommended MRO for this mixin is

    1. local customizations
    2. dialect-specific compiler
    3. CompilerPrettyMixin
    4. SQLCompiler

    This avoids overriding dialect-specific code with the prettification code,
    which would lead to potentially very unclear bugs.

    To cacth the inverse problem, i.e. dialect-specific code overriding the
    prettification code, on subclass creation, the `__init_subclass__` hook
    checks that all prettification methods were either not overridden,
    or were last overridden in a local customization (any `bi_…` python package).
    """

    _pretty: Formatter = Formatter()

    @staticmethod
    def _is_local_overrides_class(supcls):
        if supcls.__module__.startswith("dl_") or supcls.__module__.startswith("bi_"):
            return True
        if supcls.__module__.startswith("tests") or supcls.__module__.endswith("_tests"):
            return True
        if supcls.__module__.startswith("__tests__"):
            return True
        return False

    @classmethod
    def __init_subclass__(cls) -> None:
        prettifier_attrs = _cls_keys(CompilerPrettyMixin)
        full_mro = cls.__mro__
        prettifier_idx = full_mro.index(CompilerPrettyMixin)
        mro = full_mro[:prettifier_idx]
        non_bi_idx = next(
            (idx for idx, supcls in enumerate(mro) if not cls._is_local_overrides_class(supcls)),
            -1,
        )
        bi_clss = mro[:non_bi_idx]
        base_clss = mro[non_bi_idx:]
        bi_attrs = {key for cls in bi_clss for key in _cls_keys(cls)}
        base_attrs = {key for cls in base_clss for key in _cls_keys(cls)}
        uncovered_attrs = (base_attrs - bi_attrs) & prettifier_attrs
        if uncovered_attrs:
            raise Exception(
                "Dialect-specific compiler overrides methods that aren't re-overridden in `bi_...`",
                dict(uncovered_attrs=uncovered_attrs, bi_clss=bi_clss, base_clss=base_clss),
            )
        super().__init_subclass__()

    def visit_join(self, join, asfrom=False, from_linter=None, **kwargs):
        if from_linter:
            from_linter.edges.update(itertools.product(join.left._from_objects, join.right._from_objects))

        if join.full:
            join_type = " FULL OUTER JOIN "
        elif join.isouter:
            join_type = " LEFT OUTER JOIN "
        # NOTE: Right join is not in the base dialect; might not be supported
        # in some databases.
        elif getattr(join, "type", None) == "right":
            join_type = " RIGHT OUTER JOIN "
        else:
            join_type = " JOIN "
        return self._pretty.join_sql_join(
            left=join.left._compiler_dispatch(self, asfrom=True, **kwargs),
            join_type=join_type,
            right=join.right._compiler_dispatch(self, asfrom=True, **kwargs),
            onclause=join.onclause._compiler_dispatch(self, **kwargs),
        )

    def visit_select(
        self,
        select_stmt,
        asfrom=False,
        insert_into=False,
        fromhints=None,
        compound_index=None,
        select_wraps_for=None,
        lateral=False,
        from_linter=None,
        **kwargs,
    ):
        assert select_wraps_for is None, (
            "SQLAlchemy 1.4 requires use of "
            "the translate_select_structure hook for structural "
            "translations of SELECT objects"
        )

        # initial setup of SELECT.  the compile_state_factory may now
        # be creating a totally different SELECT from the one that was
        # passed in.  for ORM use this will convert from an ORM-state
        # SELECT to a regular "Core" SELECT.  other composed operations
        # such as computation of joins will be performed.
        compile_state = select_stmt._compile_state_factory(select_stmt, self, **kwargs)
        select_stmt = compile_state.statement

        toplevel = not self.stack

        if toplevel and not self.compile_state:
            self.compile_state = compile_state

        is_embedded_select = compound_index is not None or insert_into

        # translate step for Oracle, SQL Server which often need to
        # restructure the SELECT to allow for LIMIT/OFFSET and possibly
        # other conditions
        if self.translate_select_structure:
            new_select_stmt = self.translate_select_structure(select_stmt, asfrom=asfrom, **kwargs)

            # if SELECT was restructured, maintain a link to the originals
            # and assemble a new compile state
            if new_select_stmt is not select_stmt:
                compile_state_wraps_for = compile_state
                select_wraps_for = select_stmt
                select_stmt = new_select_stmt

                compile_state = select_stmt._compile_state_factory(select_stmt, self, **kwargs)
                select_stmt = compile_state.statement

        entry = self._default_stack_entry if toplevel else self.stack[-1]

        populate_result_map = need_column_expressions = (
            toplevel
            or entry.get("need_result_map_for_compound", False)
            or entry.get("need_result_map_for_nested", False)
        )

        # indicates there is a CompoundSelect in play and we are not the
        # first select
        if compound_index:
            populate_result_map = False

        # this was first proposed as part of #3372; however, it is not
        # reached in current tests and could possibly be an assertion
        # instead.
        if not populate_result_map and "add_to_result_map" in kwargs:
            del kwargs["add_to_result_map"]

        froms = self._setup_select_stack(select_stmt, compile_state, entry, asfrom, lateral, compound_index)

        column_clause_args = kwargs.copy()
        column_clause_args.update({"within_label_clause": False, "within_columns_clause": False})

        text_pieces = ["SELECT"]  # we're off to a good start !

        if select_stmt._hints:
            hint_text, byfrom = self._setup_select_hints(select_stmt)
            if hint_text:
                text_pieces.append(hint_text)
        else:
            byfrom = None

        if select_stmt._independent_ctes:
            for cte in select_stmt._independent_ctes:
                cte._compiler_dispatch(self, **kwargs)

        if select_stmt._prefixes:
            text_pieces.append(self._generate_prefixes(select_stmt, select_stmt._prefixes, **kwargs))

        subtext = self.get_select_precolumns(select_stmt, **kwargs)
        if subtext:
            text_pieces.append(subtext)
        # the actual list of columns to print in the SELECT column list.
        inner_columns = [
            c
            for c in [
                self._label_select_column(
                    select_stmt,
                    column,
                    populate_result_map,
                    asfrom,
                    column_clause_args,
                    name=name,
                    proxy_name=proxy_name,
                    fallback_label_name=fallback_label_name,
                    column_is_repeated=repeated,
                    need_column_expressions=need_column_expressions,
                )
                for (
                    name,
                    proxy_name,
                    fallback_label_name,
                    column,
                    repeated,
                ) in compile_state.columns_plus_names
            ]
            if c is not None
        ]

        if populate_result_map and select_wraps_for is not None:
            # if this select was generated from translate_select,
            # rewrite the targeted columns in the result map

            translate = dict(
                zip(
                    [
                        name
                        for (
                            key,
                            proxy_name,
                            fallback_label_name,
                            name,
                            repeated,
                        ) in compile_state.columns_plus_names
                    ],
                    [
                        name
                        for (
                            key,
                            proxy_name,
                            fallback_label_name,
                            name,
                            repeated,
                        ) in compile_state_wraps_for.columns_plus_names
                    ],
                )
            )

            self._result_columns = [
                (key, name, tuple(translate.get(o, o) for o in obj), type_)
                for key, name, obj, type_ in self._result_columns
            ]

        body_text = self._pretty.join(text_pieces)
        body_text = self._compose_select_body(
            body_text + " ",
            select_stmt,
            compile_state,
            inner_columns,
            froms,
            byfrom,
            toplevel,
            kwargs,
        )
        text_pieces = [body_text]

        if select_stmt._statement_hints:
            per_dialect = [
                ht for (dialect_name, ht) in select_stmt._statement_hints if dialect_name in ("*", self.dialect.name)
            ]
            if per_dialect:
                text_pieces.append(self.get_statement_hint_text(per_dialect))

        if self.ctes:
            # In compound query, CTEs are shared at the compound level
            if not is_embedded_select:
                nesting_level = len(self.stack) if not toplevel else None
                text_pieces = [self._render_cte_clause(nesting_level=nesting_level)] + text_pieces

        if select_stmt._suffixes:
            text_pieces.append(self._generate_prefixes(select_stmt, select_stmt._suffixes, **kwargs))

        self.stack.pop(-1)

        return self._pretty.join(text_pieces)

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

        if self.linting & sa.sql.compiler.COLLECT_CARTESIAN_PRODUCTS:
            from_linter = sa.sql.compiler.FromLinter({}, set())
            warn_linting = self.linting & sa.sql.compiler.WARN_LINTING
            if toplevel:
                self.from_linter = from_linter
        else:
            from_linter = None  # type: ignore
            warn_linting = False

        if froms:
            if select._hints:
                from_sql = self._pretty.join_group(
                    [
                        f._compiler_dispatch(self, asfrom=True, fromhints=byfrom, from_linter=from_linter, **kwargs)
                        for f in froms
                    ]
                )
            else:
                from_sql = self._pretty.join_group(
                    [f._compiler_dispatch(self, asfrom=True, from_linter=from_linter, **kwargs) for f in froms]
                )
            text_pieces.append(self._pretty.join_block("FROM", from_sql, block_indent=False))
        else:
            text_pieces.append(self.default_from())

        if select._where_criteria:
            t = self._generate_delimited_and_list(select._where_criteria, from_linter=from_linter, **kwargs)
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
            text_pieces.append(t)

        if select._has_row_limiting_clause:
            text_pieces.append(self._row_limit_clause(select, **kwargs))

        if select._for_update_arg is not None:
            text_pieces.append(self.for_update_clause(select, **kwargs))

        return self._pretty.join(text_pieces)

    def visit_alias(self, *args, **kwargs):
        sup = super().visit_alias(*args, **kwargs)
        sup = self._pretty.reparenthesize(sup)
        return sup

    def group_by_clause(self, select, **kw):
        sup = super().group_by_clause(select, **kw)
        return self._pretty.postprocess_block("GROUP BY", sup)

    def order_by_clause(self, select, **kw):
        sup = super().order_by_clause(select, **kw)
        return self._pretty.postprocess_block("ORDER BY", sup)

    def limit_clause(self, select, **kw):
        sup = super().limit_clause(select, **kw)
        return self._pretty.postprocess_block("LIMIT", sup)

    def visit_clauselist(self, clauselist, *args, **kwargs):
        if clauselist.operator is sa.sql.operators.and_:
            assert sa.sql.compiler.OPERATORS[clauselist.operator].strip() == "AND"
            pieces = [el._compiler_dispatch(self, *args, **kwargs) for el in clauselist.clauses]
            separator = "AND "
            pieces = pieces[:1] + [separator + piece for piece in pieces[1:]]
            return self._pretty.join_ext(pieces, extra_separator="")

        sup = super().visit_clauselist(clauselist, *args, **kwargs)
        return sup

    def _generate_delimited_list(self, elements, separator, **kw):
        if separator in (", ", " "):
            children = [el._compiler_dispatch(self, **kw) for el in elements]
            return self._pretty.join_ext(children, extra_separator=separator.strip())
        return super()._generate_delimited_list(elements, separator, **kw)
