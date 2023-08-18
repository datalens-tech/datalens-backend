from __future__ import annotations

import datetime
from functools import singledispatchmethod
from typing import Any, Union

import sqlalchemy as sa
from sqlalchemy.sql.elements import BindParameter, Cast, Null
import sqlalchemy.dialects.postgresql as sa_postgresql

from bi_formula.core.nodes import BaseLiteral
from bi_formula.core.dialect import DialectCombo


class TypeDefiningCast(Cast):
    """
    A mostly straightforward Cast subclass
    that marks the 'actual type was not changed' cases.

    Intended for telling the bindparameter type to postgres
    (in compeng asyncpg postgres prepared statements).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(self.clause, sa.sql.elements.BindParameter), 'This Cast is only meant for constants'

    @property
    def value(self):
        """
        Compatibility wrapper (mimicks the BindParameter).

        TODO: Should use `un_literal` instead of `….value` everywhere.
        """
        return self.clause.value


Literal = Union[
    BaseLiteral,  # unwrapped formula node
    # various SA wrappers and types:
    BindParameter,
    TypeDefiningCast,
    sa.sql.functions.Function,  # CH cast via toDatTime
    Null,
    sa_postgresql.array,
]


class Literalizer:
    __slots__ = ()

    @singledispatchmethod
    def _literal(self, value: Any, dialect: DialectCombo) -> Literal:
        return sa.literal(value)

    @_literal.register
    def _literal_int(self, value: int, dialect: DialectCombo) -> Literal:
        return self.literal_int(value, dialect=dialect)

    @_literal.register
    def _literal_float(self, value: float, dialect: DialectCombo) -> Literal:
        return self.literal_float(value, dialect=dialect)

    @_literal.register
    def _literal_datetime(self, value: datetime.datetime, dialect: DialectCombo) -> Literal:
        return self.literal_datetime(value, dialect=dialect)

    @_literal.register
    def _literal_date(self, value: datetime.date, dialect: DialectCombo) -> Literal:
        return self.literal_date(value, dialect=dialect)

    @_literal.register
    def _literal_bool(self, value: bool, dialect: DialectCombo) -> Literal:
        return self.literal_bool(value, dialect=dialect)

    @_literal.register
    def _literal_str(self, value: str, dialect: DialectCombo) -> Literal:
        return self.literal_str(value, dialect=dialect)

    @_literal.register(list)
    @_literal.register(tuple)
    def _literal_array(self, value: Union[tuple, list], dialect: DialectCombo) -> Literal:
        return self.literal_array(value, dialect=dialect)

    def literal_int(self, value: int, dialect: DialectCombo) -> Literal:
        return sa.literal(value)

    def literal_float(self, value: float, dialect: DialectCombo) -> Literal:
        return sa.literal(value)

    def literal_datetime(self, value: datetime.datetime, dialect: DialectCombo) -> Literal:
        return sa.literal(value)

    def literal_date(self, value: datetime.date, dialect: DialectCombo) -> Literal:
        return sa.literal(value)

    def literal_bool(self, value: bool, dialect: DialectCombo) -> Literal:
        return sa.literal(value)

    def literal_str(self, value: str, dialect: DialectCombo) -> Literal:
        return sa.literal(value)

    def literal_array(self, value: Union[tuple, list], dialect: DialectCombo) -> Literal:
        return sa.literal(value)

    def literal(self, value: Any, dialect: DialectCombo) -> Literal:
        return self._literal(value, dialect=dialect)
