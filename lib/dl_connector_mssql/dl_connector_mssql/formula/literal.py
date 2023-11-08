import datetime

import sqlalchemy as sa
import sqlalchemy.dialects.mssql.base as sa_mssql

from dl_formula.connectors.base.literal import (
    Literal,
    Literalizer,
)
from dl_formula.core.dialect import DialectCombo


class MSSQLLiteralizer(Literalizer):
    __slots__ = ()

    def literal_datetime(self, value: datetime.datetime, dialect: DialectCombo) -> Literal:
        return sa.cast(value.isoformat(), sa_mssql.DATETIMEOFFSET)
