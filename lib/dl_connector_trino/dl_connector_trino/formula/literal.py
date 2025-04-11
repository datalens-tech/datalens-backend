import datetime
from typing import (
    Union,
    cast,
)

import sqlalchemy as sa

from dl_formula.connectors.base.literal import (
    Literal,
    Literalizer,
)
from dl_formula.core.dialect import DialectCombo

from dl_connector_trino.formula.definitions.custom_constructors import TrinoArray


class TrinoLiteralizer(Literalizer):
    def literal_datetime(self, value: datetime.datetime, dialect: DialectCombo) -> Literal:
        datetime_repr = value.strftime("%Y-%m-%d %H:%M:%S")
        if value.microsecond:
            datetime_repr += f".{value.microsecond:06}"

        if value.tzinfo is None:
            return sa.text(f"TIMESTAMP '{datetime_repr}'")

        if value.tzinfo == datetime.timezone.utc:
            timezone_repr = "UTC"

        elif hasattr(value.tzinfo, "zone"):
            # This is a pytz timezone object
            timezone_repr = value.tzinfo.zone

        elif isinstance(value.tzinfo, datetime.timezone):
            # Calculate the offset in hours and minutes
            total_seconds = int(value.tzinfo.utcoffset(value).total_seconds())
            sign = "+" if total_seconds >= 0 else "-"
            total_seconds = abs(total_seconds)
            hours, remainder = divmod(total_seconds, 3600)
            minutes = remainder // 60
            timezone_repr = f"{sign}{hours:02}:{minutes:02}"

        else:
            raise TypeError(f"Unsupported tzinfo type: {type(value.tzinfo)}")

        return sa.text(f"TIMESTAMP '{datetime_repr} {timezone_repr}'")

    def literal_array(self, value: Union[tuple, list], dialect: DialectCombo) -> Literal:
        return cast(Literal, TrinoArray(*value))
