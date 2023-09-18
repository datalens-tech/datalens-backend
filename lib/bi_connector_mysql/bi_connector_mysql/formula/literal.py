import datetime

import sqlalchemy as sa
import sqlalchemy.dialects.mysql.base as sa_mysql

from dl_formula.connectors.base.literal import (
    Literal,
    Literalizer,
)
from dl_formula.core.dialect import DialectCombo


class MySQLLiteralizer(Literalizer):
    __slots__ = ()

    def literal_datetime(self, value: datetime.datetime, dialect: DialectCombo) -> Literal:
        return sa.cast(value.isoformat(), sa_mysql.DATETIME(fsp=6))

    def literal_date(self, value: datetime.date, dialect: DialectCombo) -> Literal:
        return sa.cast(value, sa.Date())
