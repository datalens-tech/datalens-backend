from typing import (
    Any,
    Type,
    Union,
)

import attr
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_postgresql
from sqlalchemy.sql.sqltypes import Integer

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D
from dl_formula.connectors.base.literal import (
    Literal,
    Literalizer,
    TypeDefiningCast,
)
from dl_formula.core.dialect import DialectCombo


PG_SMALLINT_MIN = -32768
PG_SMALLINT_MAX = 32767
PG_INTEGER_MIN = -2147483648
PG_INTEGER_MAX = 2147483647


def integer_to_pg_sa_type(value: int) -> Type[Integer]:
    # Some operations are not supported on longer ints, so optimize
    if PG_SMALLINT_MIN <= value <= PG_SMALLINT_MAX:
        return sa.SMALLINT
    if PG_INTEGER_MIN <= value <= PG_INTEGER_MAX:
        return sa.INTEGER
    return sa.BIGINT  # int64 in pg


class BasePostgreSQLLiteralizer(Literalizer):
    __slots__ = ()

    def literal_array(self, value: Union[tuple, list], dialect: DialectCombo) -> Literal:
        return sa_postgresql.array(value)


class MainPostgreSQLLiteralizer(BasePostgreSQLLiteralizer):
    __slots__ = ()

    def literal_int(self, value: int, dialect: DialectCombo) -> Literal:
        sa_type = integer_to_pg_sa_type(value)
        return sa.literal(value, sa_type)


class CompengPostgreSQLLiteralizer(BasePostgreSQLLiteralizer):
    __slots__ = ()

    def literal_float(self, value: float, dialect: DialectCombo) -> Literal:
        sa_type = sa_postgresql.DOUBLE_PRECISION
        return TypeDefiningCast(value, sa_type)

    def literal_int(self, value: int, dialect: DialectCombo) -> Literal:
        sa_type = integer_to_pg_sa_type(value)
        return TypeDefiningCast(value, sa_type)

    def literal_bool(self, value: bool, dialect: DialectCombo) -> Literal:
        sa_type = sa.BOOLEAN
        return TypeDefiningCast(value, sa_type)

    def literal_str(self, value: str, dialect: DialectCombo) -> Literal:
        sa_type = sa.TEXT
        return TypeDefiningCast(value, sa_type)


@attr.s(frozen=True)
class GenericPostgreSQLLiteralizer(Literalizer):
    lit_compeng = attr.ib(init=False, factory=CompengPostgreSQLLiteralizer)
    lit_main = attr.ib(init=False, factory=MainPostgreSQLLiteralizer)

    def literal(self, value: Any, dialect: DialectCombo) -> Literal:
        if dialect & D.COMPENG == dialect:
            return self.lit_compeng.literal(value, dialect=dialect)

        return self.lit_main.literal(value, dialect=dialect)
