from __future__ import annotations

import datetime
from typing import (
    Optional,
    Union,
)

import dateutil.parser
import dateutil.relativedelta


def now():  # type: ignore  # 2024-01-29 # TODO: Function is missing a return type annotation  [no-untyped-def]
    return datetime.datetime.utcnow().replace(microsecond=0)


def today():  # type: ignore  # 2024-01-29 # TODO: Function is missing a return type annotation  [no-untyped-def]
    return datetime.datetime.utcnow().date()


hour = datetime.timedelta(hours=1)
day = datetime.timedelta(days=1)
month = dateutil.relativedelta.relativedelta(months=1)
year = dateutil.relativedelta.relativedelta(years=1)


class approximately:
    def __init__(self, value, rel_tol=1e-09, abs_tol=1e-09):  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation  [no-untyped-def]
        self._value = value
        self._rel_tol = rel_tol
        self._abs_tol = abs_tol

    def __eq__(self, other):  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation  [no-untyped-def]
        other = float(other)
        return abs(self._value - other) <= max(self._rel_tol * max(abs(self._value), abs(other)), self._abs_tol)


class approx_datetime:
    def __init__(self, value):  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation  [no-untyped-def]
        self._value = value

    def __eq__(self, other):  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation  [no-untyped-def]
        return abs(self._value - other) <= datetime.timedelta(seconds=2)


class approx_timestamp(approximately):
    def __init__(self, value):  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation  [no-untyped-def]
        super().__init__(value, abs_tol=1)


def to_str(v) -> str:  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
    if isinstance(v, bytes):
        return v.decode("ascii")
    return v


def to_unicode(v) -> str:  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
    if isinstance(v, bytes):
        return v.decode("utf-8")
    return v


def to_datetime(date: datetime.date) -> datetime.datetime:
    return datetime.datetime(date.year, date.month, date.day)


def dt_strip(value: Union[datetime.datetime, str]) -> datetime.datetime:
    if isinstance(value, str):
        value = dateutil.parser.parse(value)
    assert isinstance(value, datetime.datetime)
    return value.replace(microsecond=0, tzinfo=None)


def utcize(dt: datetime.datetime) -> datetime.datetime:
    return dt.replace(tzinfo=datetime.timezone.utc)


def utc_ts(*args, tzinfo: Optional[datetime.tzinfo] = None) -> float:  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
    if len(args) == 1:
        dt = args[0]
        if not isinstance(dt, datetime.datetime):
            dt = to_datetime(dt)
    elif len(args) >= 3:
        dt = datetime.datetime(*args)
    else:
        raise ValueError(args)

    return utcize(dt).timestamp()


def as_tz(dt: datetime.datetime, tzinfo: datetime.tzinfo) -> datetime.datetime:
    assert hasattr(tzinfo, "localize"), "non-pytz time zones are not supported"
    # Sadly pytz has no single base class defining the `localize` interface
    return tzinfo.localize(dt)  # type: ignore
