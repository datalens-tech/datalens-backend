from __future__ import annotations

from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Type,
)

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_postgresql
from sqlalchemy.sql.elements import Null
from sqlalchemy.sql.type_api import TypeEngine

from dl_formula.connectors.base.literal import (
    Literal,
    Literalizer,
    TypeDefiningCast,
)
from dl_formula.core.dialect import DialectCombo
from dl_formula.core.nodes import BaseLiteral
from dl_formula.utils.datetime import make_datetime_value


if TYPE_CHECKING:
    from dl_formula.translation.context import TranslationCtx


_LITERALIZER_REGISTRY: dict[DialectCombo, Literalizer] = {}


@lru_cache
def get_literalizer(dialect: DialectCombo) -> Literalizer:
    sorted_dialect_items = sorted(
        _LITERALIZER_REGISTRY.items(), key=lambda el: el[0].ambiguity
    )  # Most specific dialects go first, the most ambiguous ones go last
    for d, literalizer in sorted_dialect_items:
        if d & dialect == dialect:
            return literalizer
    return Literalizer()  # FIXME: remove fallback after all dialects become connectors


def register_literalizer(dialect: DialectCombo, literalizer_cls: Type[Literalizer]) -> None:
    if dialect in _LITERALIZER_REGISTRY:
        assert isinstance(_LITERALIZER_REGISTRY[dialect], literalizer_cls)
        return

    _LITERALIZER_REGISTRY[dialect] = literalizer_cls()


def literal(value: Any, type_: Optional[TypeEngine] = None, d: Optional[DialectCombo] = None) -> Literal:
    """A fine-tuned version of ``sqlalchemy.literal``"""
    dialect = d
    if type_ is None and dialect is not None:
        literalizer = get_literalizer(dialect=dialect)
        return literalizer.literal(value, dialect=dialect)

    return sa.literal(value, type_)


def un_literal(sa_obj: Optional[Literal], value_ctx: Optional[TranslationCtx] = None):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
    """Given an SA object produced by `literal` (or compatible), return the original value"""

    assert sa_obj is not None

    if value_ctx is not None and isinstance(value_ctx.node, BaseLiteral):
        return value_ctx.node.value

    if isinstance(sa_obj, sa.sql.elements.BindParameter):
        return sa_obj.value

    if isinstance(sa_obj, TypeDefiningCast):
        return sa_obj.clause.value

    if isinstance(sa_obj, Null):
        return None

    if isinstance(sa_obj, sa_postgresql.array):
        return [un_literal(clause) for clause in sa_obj.clauses]

    # More complex expressions via functions
    if isinstance(sa_obj, sa.sql.functions.Function):
        clauses = sa_obj.clauses.clauses
        if all(isinstance(clause, (sa.sql.elements.BindParameter, Null)) for clause in clauses):
            if sa_obj.name == "toDateTime":
                # CH datetime un-wrap
                if len(clauses) == 2:
                    dt_str = un_literal(clauses[0])
                    return make_datetime_value(dt_str)
            if sa_obj.name == "array":
                return [un_literal(clause) for clause in clauses]

    raise ValueError(f"Not a literal-like SA object: {sa_obj!r}")


def is_literal(sa_obj) -> bool:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
    try:
        un_literal(sa_obj)
    except ValueError:
        return False
    return True
