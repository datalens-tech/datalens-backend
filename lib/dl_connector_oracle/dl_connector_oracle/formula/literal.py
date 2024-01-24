import sqlalchemy as sa

from dl_formula.connectors.base.literal import (
    Literal,
    Literalizer,
)
from dl_formula.core.dialect import DialectCombo


class OracleLiteralizer(Literalizer):
    __slots__ = ()

    def literal_bool(self, value: bool, dialect: DialectCombo) -> Literal:
        return sa.literal(int(value))

    def literal_str(self, value: str, dialect: DialectCombo) -> Literal:
        # SA uses NVARCHAR2 for string literals by default,
        # but CHAR has better compatibility with some of the functions, so try to use it
        # with a fallback to NCHAR for non-ASCII strings
        try:
            value.encode("ascii")
            type_ = sa.CHAR(len(value))
        except UnicodeEncodeError:
            type_ = sa.NCHAR(len(value))  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "NCHAR", variable has type "CHAR")  [assignment]
        return sa.literal(value, type_)
