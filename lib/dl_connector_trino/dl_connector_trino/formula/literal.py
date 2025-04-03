import datetime
from typing import Union

import sqlalchemy as sa
from sqlalchemy.sql.expression import TextClause
from trino.sqlalchemy.datatype import TIMESTAMP

from dl_formula.connectors.base.literal import (
    Literal,
    Literalizer,
)
from dl_formula.core.dialect import DialectCombo


def trino_array(values: list) -> TextClause:
    binds = [sa.bindparam(f"v_{i}", v) for i, v in enumerate(values)]
    array_sql = "ARRAY[" + ",".join(f":{b.key}" for b in binds) + "]"
    return sa.text(array_sql).bindparams(*binds)


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
        return trino_array(value)
