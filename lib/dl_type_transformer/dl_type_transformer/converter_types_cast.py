from __future__ import annotations

import datetime
import re
from typing import (
    Any,
    Iterable,
    Optional,
)

import dateutil.parser

spaces_in_numbers_re = re.compile(r"(?<=\d) (?=\d)")


def _to_int(value: Any) -> int:
    if isinstance(value, str):
        if " " in value:
            value = value.replace(" ", "")
    return int(value)


def _to_float(value: Any) -> float:
    if isinstance(value, str):
        if "," in value:
            value = value.replace(",", ".")
        if " " in value:
            value = spaces_in_numbers_re.sub("", value)
    return float(value)


def _to_date(value: str, formats: Optional[Iterable[str]] = None) -> datetime.date:
    # TODO?: try `ciso8601.parse_datetime`, for correct ISO-8601, timezones, speed, etc.
    formats = formats or (
        "%Y-%m-%d",
        # weird stuff
        "%d.%m.%y",
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%d/%m/%y",
        # day-start:
        "%Y-%m-%d 00:00:00",
        "%Y-%m-%d 00:00:00.000000",
    )
    dt = None
    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(value, fmt)
        except ValueError:
            pass
        else:
            break
    if dt is None:
        raise ValueError(value)
    return dt.date()


def _to_datetime(value: str) -> datetime.datetime:
    dt = dateutil.parser.parse(value)
    return dt


def _to_boolean(value: str | bool) -> bool:
    if isinstance(value, str):
        value = value.lower()
        if value in ("true", "1"):
            return True
        elif value in ("false", "0"):
            return False
        raise ValueError(value)
    return bool(value)
