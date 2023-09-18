import datetime
from typing import Union

import sqlalchemy as sa

from dl_formula.connectors.base.literal import (
    Literal,
    Literalizer,
)
from dl_formula.core.dialect import DialectCombo

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D


class ClickHouseLiteralizer(Literalizer):
    __slots__ = ()

    def literal_datetime(self, value: datetime.datetime, dialect: DialectCombo) -> Literal:
        # TODO: DateTime64 support for fresh CHs.
        tzinfo = value.tzinfo
        value = value.replace(microsecond=0, tzinfo=None)
        if tzinfo:
            tzname = getattr(tzinfo, "zone", tzinfo.tzname(value))
            return sa.func.toDateTime(value.isoformat(), tzname)
        else:
            return sa.func.toDateTime(value.isoformat())

    def literal_date(self, value: datetime.date, dialect: DialectCombo) -> Literal:
        if dialect & D.and_above(D.CLICKHOUSE_22_10):
            return sa.func.toDate32(value.isoformat())
        return sa.func.toDate(value.isoformat())

    def literal_bool(self, value: bool, dialect: DialectCombo) -> Literal:
        return sa.literal(int(value))

    def literal_array(self, value: Union[tuple, list], dialect: DialectCombo) -> Literal:
        return sa.func.array(*value)
