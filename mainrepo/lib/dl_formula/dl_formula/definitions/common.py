from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.sql.elements import (
    UnaryExpression,
    WithinGroup,
)
from sqlalchemy.sql.operators import desc_op

from dl_formula.core import (
    exc,
    nodes,
)
from dl_formula.shortcuts import n

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import ClauseElement


class _TextClauseHack(sa.sql.elements.TextClause):
    def __bool__(self):
        # Possibly a bug in sqlalchemy 1.4: query caching fails as, normally,
        # 'Boolean value of this clause is not defined'.
        # Not that the cache should be used here anyway, most likely.
        return True


def raw_sql(sql_text):
    """Basically just `sa.text(sql_text)` with hacks"""
    return _TextClauseHack(sql_text)


def make_binary_chain(binary_callable, *args, wrap_as_nodes: bool = True):
    if len(args) == 0:
        return None

    expr = args[0]
    for next_arg in args[1:]:
        binary_args = expr, next_arg
        if wrap_as_nodes:
            expr = binary_callable(*binary_args)
        else:
            expr = binary_callable(
                *[
                    n.lit(arg) if wrap_as_nodes and not isinstance(arg, nodes.FormulaItem) else arg
                    for arg in binary_args
                ]
            )
    return expr


def quantile_value(quant: float):
    """
    Validate and normalize the value for a quantile.
    """
    # Note: disallowing the `0` and `1` for some extra unambiguity
    # (for those who might think that `1` is `1%`)
    if quant <= 0:
        raise exc.TranslationError("Invalid quantile value: value must be greater than 0")
    if quant >= 1:
        raise exc.TranslationError("Invalid quantile value: value must be less than 1")
    return quant


def desc(value: ClauseElement) -> ClauseElement:
    if isinstance(value, UnaryExpression) and value.modifier is desc_op:
        # already wrapped in desc, so just unwrap
        value = value.element
    else:
        # not wrapped in desc, so wrap it
        value = sa.desc(value)

    return value


def ifnotnull(value, expr):
    """Wrap `expr` in an `sa.case` that results in a NULL for a NULL `value`"""
    return sa.case([(value.is_(None), sa.null())], else_=expr)


class _PatchedWithinGroup(WithinGroup):
    def __reduce__(self) -> tuple:
        return self.__class__, (self.element, *self.order_by)  # type: ignore


def within_group(clause_el: ClauseElement, *order_by) -> _PatchedWithinGroup:
    return _PatchedWithinGroup(clause_el, *order_by)
