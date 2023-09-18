import datetime

import sqlalchemy as sa

from dl_formula.connectors.base.literal import (
    Literal,
    Literalizer,
)
from dl_formula.core.dialect import DialectCombo


class SnowFlakeLiteralizer(Literalizer):
    __slots__ = ()

    def literal_datetime(self, value: datetime.datetime, dialect: DialectCombo) -> Literal:
        return sa.func.TO_TIMESTAMP(value)

    def literal_date(self, value: datetime.date, dialect: DialectCombo) -> Literal:
        return sa.func.TO_DATE(value)
