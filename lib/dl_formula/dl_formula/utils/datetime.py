from __future__ import annotations

import datetime
from typing import Union

import ciso8601
import pytz
import pytz.tzinfo


# TODO: re-check and document the useful differences between
# `datetime.timezone.utc` and `pytz.utc`.
UTC = datetime.timezone.utc


SOME_DT = datetime.datetime(2020, 1, 1, 0, 0, 0)


DT_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%d %H",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    # To support e.g. '#2019-01-01T#'. Dubious.
    "%Y-%m-%d ",
)


DT_FORMATS_BY_LENGTH = {
    # length: strptime_format
    # assumes preprocessed 'T'
    len(SOME_DT.strftime(fmt)): fmt
    for fmt in DT_FORMATS
}


def get_tzobj(tzname: str) -> datetime.tzinfo:
    try:
        # Should essentially be a hashmap lookup, no need to cache it further.
        return pytz.timezone(tzname)
    except pytz.UnknownTimeZoneError:
        raise ValueError(f"Invalid timezone name: {tzname!r}")


def parse_dt_string(value: str) -> datetime.datetime:
    simple_format = DT_FORMATS_BY_LENGTH.get(len(value))
    if simple_format is not None:
        value = value.replace("T", " ").replace("t", " ")
        result = datetime.datetime.strptime(value, simple_format)  # raises ValueError
    else:
        result = ciso8601.parse_datetime(value)  # raises ValueError

    return result


DTSourcesT = Union[None, str, int, float, datetime.date, datetime.datetime]


def make_datetime_value_base(value: DTSourcesT) -> datetime.datetime:
    """Convert a value into a datetime object, without tzinfo or with a fixed offset"""
    if isinstance(value, str):
        return parse_dt_string(value)
    if isinstance(value, (int, float)):
        return datetime.datetime.utcfromtimestamp(value)
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time())
    raise Exception("Unexpected value type", dict(value_type=type(value)))


def make_datetime_value(value: DTSourcesT) -> datetime.datetime:
    """Convert a value into a datetime object, without tzinfo or at UTC"""
    dt = make_datetime_value_base(value)
    if dt.tzinfo is None:
        return dt
    # There's no known use for an offset at this point,
    # so ensure the result is at UTC.
    return dt.astimezone(UTC)


def make_datetimetz_value(value: DTSourcesT, tzname: str = "UTC") -> datetime.datetime:
    """Convert a value into a datetime object, at UTC, using tzname to interpret naive datetimes"""
    tzobj = get_tzobj(tzname)  # also pre-validates `tzname`
    assert isinstance(tzobj, (pytz.tzinfo.DstTzInfo, pytz.tzinfo.StaticTzInfo))
    dt = make_datetime_value(value)
    assert dt is not None
    if dt.tzinfo is None:
        dt = tzobj.localize(dt)
    dt = dt.astimezone(UTC)
    return dt
