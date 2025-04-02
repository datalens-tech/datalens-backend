import datetime
from typing import Union

import sqlalchemy as sa
import trino.sqlalchemy.datatype as tsa
from trino.sqlalchemy.datatype import TIMESTAMP

from dl_formula.connectors.base.literal import (
    Literal,
    Literalizer,
)
from dl_formula.core.dialect import DialectCombo

from dl_connector_trino.formula.definitions.custom_constructors import TrinoConstArray


class TrinoLiteralizer(Literalizer):
    def literal_datetime(self, value: datetime.datetime, dialect: DialectCombo) -> Literal:
        precision = 6 if value.microsecond else None
        has_tz = value.tzinfo is not None

        if has_tz and not isinstance(value.tzinfo, datetime.timezone):
            utc_offset = value.utcoffset()
            assert utc_offset is not None
            value = value.replace(tzinfo=datetime.timezone(utc_offset))

        return sa.cast(
            value,
            TIMESTAMP(
                timezone=has_tz,
                precision=precision,
            ),
        )

    def literal_array(self, value: Union[tuple, list], dialect: DialectCombo) -> Literal:
        if value and any(isinstance(x, float) for x in value):
            return sa.literal_column(sa.cast(TrinoConstArray(*value), tsa.DOUBLE()))

        return sa.literal_column(TrinoConstArray(*value))
